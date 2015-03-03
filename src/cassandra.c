#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <malloc.h>

#include "cassandra.h"

/*-- Header files for hashmap implementation --*/
#include "cfuhash.h"

/* -- Following are the declarations / initializations for ResultSet Object --*/

result_set *create_rs()
{
	return (result_set*) malloc (sizeof(result_set));
}

void result_set_destroy(result_set *rs)
{
	int i;

	cfuhash_destroy(rs->map);
	free(rs->schema_name);
	free(rs->buffer);
	free(rs->table_name);
	for (i = 0; i < rs->column_count; i++)
		free(rs->col_desc_array[i].name);
	free(rs->col_desc_array);
	free(rs);
}

void init_rs(result_set *rs)
{
	rs->map = cfuhash_new();
	rs->buffer = NULL;
	rs->column_count = 0;
	rs->row_count = 0;
	rs->curr_pos = 0;
	rs->curr_row = 0;
	rs->col_desc_array = NULL;
	rs->table_name = NULL;
	rs->schema_name = NULL;
}

/*------------------------------------------------*/

void int32_to_uint8(uint8_t *output,int n)
{
	int i;
	for(i = 0; i < 4; i++)
	{
		output[i] = n >> ((3 - i) * 8) & 0xff; 
	}
}

int uint8_to_int32(uint8_t *input)
{
	int i = (input[0] << 24) | (input[1] << 16) | (input[2] << 8) | input[3];
	return i;
}

int uint8_to_int16(uint8_t *input)
{
	int i = (input[0] << 16) | (input[1]);
	return i;
}

void uint8_to_string(uint8_t *in, int n, char *output)
{
	int i;
	for (i = 0; i < n; i++)
		output[i] = in[i];
}

void *get_val(result_set *rs, char *column_name)
{
	return cfuhash_get(rs->map, column_name);
}

int has_next(result_set *rs)
{
	int m, x, p, q, j, *val;
	char *output;

	printf("%d\n",rs->curr_row);
	rs->curr_row++;

	if (rs->curr_row > rs->row_count)
		return 0;

	cfuhash_clear(rs->map);

	for (m = 0; m < rs->column_count; m++)
	{
		x = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
		rs->curr_pos += 4;

		if (x < 0)
		{
			continue;
		}
		if (rs->col_desc_array[m].type == VARCHAR)
		{
			output = (char *)malloc(x + 1);

			uint8_to_string(&rs->buffer[rs->curr_pos], x, output);
			output[x] = '\0';

			rs->curr_pos += x;

			cfuhash_put(rs->map, rs->col_desc_array[m].name, output);
		}
		else if (rs->col_desc_array[m].type == UUID)
		{
			output = (char *)malloc(x + 1);

			/* Replace this by uint8_to_string */
			printf("Column Value");
			for(j = 0; j < x; j++, rs->curr_pos++)
			{
				printf("%x", rs->buffer[rs->curr_pos]);
				output[j] = rs->buffer[rs->curr_pos];
			}
			output[j] = '\0';
			printf("\n");
			cfuhash_put(rs->map, rs->col_desc_array[m].name, output);
		}
		else if (rs->col_desc_array[m].type == INT)
		{
			val = (int *)malloc(sizeof(int));
			x = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
			*val = x;
			rs->curr_pos += 4;
			cfuhash_put(rs->map, rs->col_desc_array[m].name, val);
		}
		else if (rs->col_desc_array[m].type == SET)
		{
			printf("Column Value");
			x = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
			rs->curr_pos += 4;
			for(j = 0; j < x; j++)
			{
				q = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
				rs->curr_pos += 4;
				for(p = 0; p < q; p++, rs->curr_pos++)
				{
					printf("%c", rs->buffer[rs->curr_pos]);
				}
				printf("\t");
			}
		}
		else if (rs->col_desc_array[m].type == 33)//map
		{
			printf("Column Value");
			long long int p, q;
			x = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
			rs->curr_pos += 4;
			for (j = 0; j < x; j++)
			{
				q = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
				rs->curr_pos += 4;
				for (p = 0; p < q; p++, rs->curr_pos++)
				{
					printf("%x", rs->buffer[rs->curr_pos]);
				}
				printf(":");

				q = uint8_to_int32(&rs->buffer[rs->curr_pos]); 
				rs->curr_pos += 4;
				for (j = 0; j < q; j++, rs->curr_pos++)
				{
					printf("%x", rs->buffer[rs->curr_pos]);
				}
			}
		}
		else
		{
			printf("Column Value:");
			for (j = 0; j < x; j++, rs->curr_pos++)
			{
				printf("%d", rs->buffer[rs->curr_pos]);
			}
		}
	}	

	return 1;
}


int cass_connect(int sock, char *ip, char *user, char *pass)
{
	uint8_t recvBuff[1024000], sendbuff[10240];
	int i, j, n;
	signed int x;

	memset(recvBuff, '0', sizeof(recvBuff));

	sockfd = -1;

	if (sockfd < 0)
	{
		if ((sockfd = socket(AF_INET, SOCK_STREAM, 0))<0)
			return 0;
	}

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(sock);
	serv_addr.sin_addr.s_addr = inet_addr(ip);

	if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	{
		fprintf(stderr,"Error : Connect Failed\n");
		sockfd = -1;
		return 0;
	}

	/* Create and send packet */

	/* Version */
	sendbuff[0] = 0x03; 

	/* Flag - set to 0 (default). For startup, neither compression nor tracing is required. */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - 1 for STARTUP */
	sendbuff[4] = STARTUP;

	/* Length of */
	sendbuff[5] = 0;
	sendbuff[6] = 0;
	sendbuff[7] = 0;
	sendbuff[8] = 0x16;

	/* Filling body for STARTUP */
	// The body is a string map - which is a short integer n -  followed b
	// n key-value pairs...<k,v> type

	sendbuff[9] = 0x00;		// this is n - in this case n = 1;
	sendbuff[10] = 0x01;		// So we have n <k,v> pairs

	sendbuff[11] = 0x00;		// each k and v is a string. A string is of the form:
	sendbuff[12] = 0x0b;		// n (length) followed by n bytes UTF-8 representing the string

	sendbuff[13] = 'c';
	sendbuff[14] = 'q';
	sendbuff[15] = 'l';
	sendbuff[16] = '_';
	sendbuff[17] = 'v';
	sendbuff[18] = 'e';
	sendbuff[19] = 'r';
	sendbuff[20] = 's';
	sendbuff[21] = 'i';
	sendbuff[22] = 'o';
	sendbuff[23] = 'n';

	/* Following is v for the first k (CQL_VERSION) */

	sendbuff[24] = 0;
	sendbuff[25] = 5;

	sendbuff[26] = '3';
	sendbuff[27] = '.';
	sendbuff[28] = '0';
	sendbuff[29] = '.';
	sendbuff[30] = '0';

	/* Now send the packet created above */

	//bind(listenfd, (struct sockaddr*)&serv_addr,sizeof(serv_addr));
	write(sockfd, sendbuff, 31);

	while((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;
		if(recvBuff[4] == AUTHENTICATE)//upcode value
			AUTH_NEEDED = 1;
		else if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);
			fprintf(stderr, "\n");
			return 0;
		}
		else
			return 1;
		break;
	}

	if(n < 0)
	{
		printf("Read Error \n");
		return 0;
	}
	/* Version */
	sendbuff[0] = 0x03; 

	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - F for AUTH_RESPONSE */
	sendbuff[4] = AUTH_RESPONSE;

	x = strlen(user) + strlen(pass) + 2 + 4;

	int32_to_uint8(&sendbuff[5], x);

	x = x - 4;

	int32_to_uint8(&sendbuff[9], x);

	sendbuff[13] = 0;

	for(i = 14, j = 0; i < 14 + (int)strlen(user); i++, j++)
	{
		sendbuff[i] = user[j];//This is username
	}

	sendbuff[i++] = 0;
	x = i;

	for (j = 0; i < x + (int)strlen(pass); i++, j++)
	{
		sendbuff[i] = pass[j];//This is password
	}

	write(sockfd, sendbuff, i);
	while ((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;
		if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);
			fprintf(stderr, "\n");
			return 0;
		}
		for (i = 0; i < n; i++)
		{
			//	printf("%x ",recvBuff[i]);
		}
		break;
	}
	if (n < 0)
	{
		return 0;
	}
	return 1;
}

result_set *cass_execute(char *query)
{
	uint8_t recvBuff[1024000], sendbuff[10240];
	int i, j, l, m, n, x;
	result_set *rs;

	rs = create_rs();
	init_rs(rs);

	sendbuff[0] = 0x03; 

	/* Flag - set to 0 (default). For startup, neither compression nor tracing is required. */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - 7 for Query */
	sendbuff[4] = QUERY;

	x = strlen(query) + 4 + 3 + 4;

	int32_to_uint8(&sendbuff[5], x);

	x = strlen(query);

	int32_to_uint8(&sendbuff[9], x);
	for (i = 13, j = 0; i < 13 + (int)strlen(query); i++, j++)
	{
		sendbuff[i] = query[j];
	}

	/* 2 bytes consistency level */
	sendbuff[i++] = 0x00;
	sendbuff[i++] = 0x01;

	/* 1 byte flags - for now, it has been set to 4, and the next 4 bytes must be result_page_size */
	sendbuff[i++] = 0x04;

	/* 4 byte result_page_size */
	sendbuff[i++] = 0x00;
	sendbuff[i++] = 0x00;
	sendbuff[i++] = 0x03;
	sendbuff[i++] = 0xf2;

	write(sockfd, sendbuff, i);

	memset(recvBuff, 0, sizeof(recvBuff));
	while ((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;

		if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);
			fprintf(stderr, "\n");
			return 0;
		}

		for (i = 9; i < 25; i++)
		{
			//	These 16 bytes are uuid which are part of Result packet
		}

		rs->buffer = (uint8_t *)malloc(n + 1);
		rs->curr_pos = i;

		for (j = 0; j < n; j++)
			rs->buffer[j] = recvBuff[j];

		if (uint8_to_int32(&recvBuff[rs->curr_pos]) == ROWS) //Result of SELECT Query
		{
			rs->curr_pos += 4; //after Check of Type of Result as above

			if (uint8_to_int32(&recvBuff[rs->curr_pos]) & 1)
			{
				rs->curr_pos += 4;//after check metadata

				COLUMN_COUNT = uint8_to_int32(&recvBuff[rs->curr_pos]);
				rs->curr_pos += 4;

				rs->column_count = COLUMN_COUNT;
				//printf("No of Columns are %d\n", rs->column_count);

				/* Column count obtained - can initialize column_desc in rs */
				rs->col_desc_array = (column_desc*)malloc(sizeof(column_desc) * (rs->column_count));
				x = uint8_to_int16(&recvBuff[rs->curr_pos]);		 
				rs->curr_pos += 2;

				rs->schema_name = (char *)malloc(x + 1);

				strncpy(rs->schema_name, (char *)&recvBuff[rs->curr_pos], x);
				rs->schema_name[x] = '\0';
				rs->curr_pos += x;

				//printf("Schema name : %s\n", rs->schema_name);

				x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;
				rs->table_name = (char *) malloc(x + 1);

				strncpy(rs->table_name, (char *)&recvBuff[rs->curr_pos], x);
				rs->table_name[x] = '\0';
				rs->curr_pos += x;

				//printf("Table name : %s\n", rs->table_name);
			}

			for (l = 0; l < COLUMN_COUNT; l++)
			{
				x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;

				rs->col_desc_array[l].name = (char *)malloc(x + 1);
				strncpy(rs->col_desc_array[l].name, (char *)&recvBuff[rs->curr_pos], x);
				rs->col_desc_array[l].name[x] = '\0';
				rs->curr_pos += x;

				//REACHED HERE	
				j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;
				rs->col_desc_array[l].type = j;

				if (j == CUSTOM)
				{
					x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
					rs->curr_pos += 2;

					printf("Printing Class name of CUSTOM type\n");
					for (m = 0; m < x; m++, rs->curr_pos++)
						printf("%c", recvBuff[rs->curr_pos]);
					printf("\n");
				}
				else if (j == SET)//uint8 0x22
				{
					/* CHECK!! Shouldn't this be a key-value extract? */
					j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
					rs->curr_pos += 2;
				}
				else if (j == MAP)//uint8 0x22
				{
					j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
					rs->curr_pos += 2;

					j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
					rs->curr_pos += 2;
				}
				else
				{
					//printf("Type : %x\n",j);
				}
			}
			ROW_COUNT = uint8_to_int32(&recvBuff[rs->curr_pos]);
			rs->curr_pos += 4;
			rs->row_count = ROW_COUNT;
		}
		break;
	}

	return rs;
}
void add_to_batch_simple(cass_batch *batch,char *str)
{
	batch->count++;
	int j = batch->curr_pos,i;		
	batch->queries[j++] = 0;
	int x = strlen(str);
	uint8_t *output = malloc(4);
	int32_to_uint8(output,x);
	batch->queries[j++] = output[0];
	batch->queries[j++] = output[1];
	batch->queries[j++] = output[2];
	batch->queries[j++] = output[3];
	for(i = 0;i<x;i++){
		batch->queries[j++] = str[i];
		printf("%c",batch->queries[j]);
	}
	batch->queries[j++] = 0;
	batch->queries[j++] = 0;
	batch->curr_pos =j;
}
int cass_execute_batch(cass_batch *batch)
{
	//printf("%d\n",batch->count);
	uint8_t recvBuff[1024], sendbuff[1024000];
	int n;
	sendbuff[0] = 0x03;

	/* Flag - set to 2, for tracing */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - 9 for Prepare */
	sendbuff[4] = BATCH;

	/* fill in length */
//	printf("%d\n",batch->count);
	int32_to_uint8(&sendbuff[5], batch->curr_pos+6);
	sendbuff[9] = 0x00;
	int32_to_uint8(&sendbuff[10], batch->count);
	sendbuff[10] = sendbuff[12];
	sendbuff[11] = sendbuff[13];
	printf("%d %d\n",sendbuff[10],sendbuff[11]);
	int i = 12,j;
	for(j=0;j<batch->curr_pos;i++,j++)
	{
		sendbuff[i] = batch->queries[j];
		printf("%c ",sendbuff[i]);
	}
	sendbuff[i++] = 0x00;
	sendbuff[i++] = 0x00;
	sendbuff[i++] = 0x40;

	write(sockfd, sendbuff, i);
	for(j = 0;j<i;j++)
		printf("%d ",sendbuff[j]);
	printf("\n");
	while ((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;
		printf(" Hello %d\n",n);
		if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);

			fprintf(stderr, "\n");
			return 0;
		}
		else if(recvBuff[4] == RESULT){
			printf("Done\n");
		break;
		}
	}
	return 1;
}
cass_prepared_statement *cass_prepare_statement(char *str)
{
	uint8_t recvBuff[1024000], sendbuff[10240];
	int query_length, i, n, curr_pos;
	int16_t id_length;
	int result_type;
	cass_prepared_statement *cps;


	sendbuff[0] = 0x03; 

	/* Flag - set to 2, for tracing */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - 9 for Prepare */
	sendbuff[4] = PREPARE;

	/* fill in length */
	query_length = strlen(str);

	int32_to_uint8(&sendbuff[5], query_length + 4);

	int32_to_uint8(&sendbuff[9], query_length);
	/* fill in the query */
	for (i = 0; i < query_length; i++)
		sendbuff[i + 13] = str[i];

	write(sockfd, sendbuff, query_length + 13);

	while ((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;

		if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);

			fprintf(stderr, "\n");
			return 0;
		}

		break;
	}

	if (n < 0)
	{
		printf("Read Error \n");
		return 0;
	}

	/* skip 16-byte UUID */
	curr_pos = 25;

	result_type = uint8_to_int32(&recvBuff[curr_pos]);
	curr_pos += 4;

	if (result_type == PREPARED)
		/*printf("Definitely result of a PREPARE message\n")*/;
	else
	{
		printf("Something went wrong. Did not receive the response for a prepared query.\n");
		return NULL;
	}

	cps = (cass_prepared_statement *)malloc(sizeof(cass_prepared_statement));

	id_length = uint8_to_int16(&recvBuff[curr_pos]);
	curr_pos += 2;

	cps->id_length = id_length;
	cps->id = (uint8_t *)malloc(id_length);

	for (i = 0; i < id_length; i++)
		cps->id[i] = recvBuff[curr_pos + i];

	return cps;
}

result_set *cass_execute_prepared_statement(cass_prepared_statement *cps)
{
	uint8_t recvBuff[1024000], sendbuff[10240];
	int i, n, curr_pos, body_length, j, x, l, m;

	result_set *rs;

	rs = create_rs();
	init_rs(rs);

	/* Sending an EXECUTE message */

	sendbuff[0] = 0x03; 

	/* Flag - set to 2, for tracing */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - A for Execute */
	sendbuff[4] = EXECUTE;

	/* Now [short bytes] id */
	sendbuff[9] = (cps->id_length) >> 8;
	sendbuff[10] = (cps->id_length);

	for (i = 0; i < cps->id_length; i++)
		sendbuff[11 + i] = cps->id[i];

	curr_pos = 11 + cps->id_length;

	/* Now add the query parameters */

	/* consistency */
	sendbuff[curr_pos++] = 0;
	sendbuff[curr_pos++] = 1;

	/* flags */
	sendbuff[curr_pos++] = 0;

	body_length = curr_pos - 9; //something - do update

	/* Length of the packet */
	int32_to_uint8(&sendbuff[5], body_length);

	write(sockfd, sendbuff, curr_pos);

	while ((n = read(sockfd, recvBuff, sizeof(recvBuff) - 1)) > 0)
	{
		recvBuff[n] = 0;

		if(recvBuff[4] == ERROR)
		{
			for (i = 9; i < n; i++)
				fprintf(stderr, "%c", recvBuff[i]);
			fprintf(stderr, "\n");
			return 0;
		}

		break;
	}

	if (n < 0)
	{
		printf("Read Error \n");
		return 0;
	}

	body_length = uint8_to_int32(&recvBuff[5]);
	//printf("Body_length is %d\n", body_length);

	rs->buffer = (uint8_t *)malloc(n + 1);
	/* Skip the tracing id */
	rs->curr_pos = 25;

	for (j = 0; j < n; j++)
		rs->buffer[j] = recvBuff[j];

	if (uint8_to_int32(&recvBuff[rs->curr_pos]) == ROWS) //Result of SELECT Query
	{
		rs->curr_pos += 4; //after Check of Type of Result as above

		if (uint8_to_int32(&recvBuff[rs->curr_pos]) & 1)
		{
			rs->curr_pos += 4;//after check metadata

			COLUMN_COUNT = uint8_to_int32(&recvBuff[rs->curr_pos]);
			rs->curr_pos += 4;

			rs->column_count = COLUMN_COUNT;
			//printf("No of Columns are %d\n", rs->column_count);

			/* Column count obtained - can initialize column_desc in rs */
			rs->col_desc_array = (column_desc*)malloc(sizeof(column_desc) * (rs->column_count));
			x = uint8_to_int16(&recvBuff[rs->curr_pos]);		 
			rs->curr_pos += 2;

			rs->schema_name = (char *)malloc(x + 1);

			strncpy(rs->schema_name, (char *)&recvBuff[rs->curr_pos], x);
			rs->schema_name[x] = '\0';
			rs->curr_pos += x;

			//printf("Schema name : %s\n", rs->schema_name);

			x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
			rs->curr_pos += 2;
			rs->table_name = (char *) malloc(x + 1);

			strncpy(rs->table_name, (char *)&recvBuff[rs->curr_pos], x);
			rs->table_name[x] = '\0';
			rs->curr_pos += x;

			//printf("Table name : %s\n", rs->table_name);
		}

		for (l = 0; l < COLUMN_COUNT; l++)
		{
			x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
			rs->curr_pos += 2;

			rs->col_desc_array[l].name = (char *)malloc(x + 1);
			strncpy(rs->col_desc_array[l].name, (char *)&recvBuff[rs->curr_pos], x);
			rs->col_desc_array[l].name[x] = '\0';
			rs->curr_pos += x;

			//REACHED HERE	
			j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
			rs->curr_pos += 2;
			rs->col_desc_array[l].type = j;

			if (j == CUSTOM)
			{
				x = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;

				printf("Printing Class name of CUSTOM type\n");
				for (m = 0; m < x; m++, rs->curr_pos++)
					printf("%c", recvBuff[rs->curr_pos]);
				printf("\n");
			}
			else if (j == SET)//uint8 0x22
			{
				/* CHECK!! Shouldn't this be a key-value extract? */
				j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;
			}
			else if (j == MAP)//uint8 0x22
			{
				j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;

				j = uint8_to_int16(&recvBuff[rs->curr_pos]); 
				rs->curr_pos += 2;
			}
			else
			{
				//printf("Type : %x\n",j);
			}
		}
		ROW_COUNT = uint8_to_int32(&recvBuff[rs->curr_pos]);
		rs->curr_pos += 4;
		rs->row_count = ROW_COUNT;
	}

	return rs;
}
