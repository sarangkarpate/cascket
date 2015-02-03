#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <arpa/inet.h>
#include "cassandra.h"
void int32_to_uint8(uint8_t *output,int n)
{
	int i;
	for(i = 0;i<4;i++){
		output[i]=n >> ((3-i)*8)&0xff; 
	}
}
int uint8_to_int32(uint8_t *input){
	int i = (input[0] <<  24) | (input[1] << 16) | (input[2] << 8) | input[3];
	return i;
}
int uint8_to_int16(uint8_t *input){
	int i = (input[0] << 16) | (input[1]);
	return i;
}
<<<<<<< HEAD
int cassandra_connect(int sock,char *ip,char *user, char*pass){
=======
int cassandra_authenticate(char *user, char*pass){
	if(AUTH_NEEDED == 0)
		return 1;
	int i,n;
	/* Version */
	sendbuff[0] = 0x03; 

	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - F for AUTH_RESPONSE */
	sendbuff[4] = 0x0F;
	signed int x = strlen(user)+strlen(pass)+2+4;
	int32_to_uint8(&sendbuff[5],x);
	x = x-4;
	int32_to_uint8(&sendbuff[9],x);
	sendbuff[13]=0;
	int j;
	for(i = 14,j=0;i<14+strlen(user);i++,j++){
		sendbuff[i] = user[j];//This is username
	}
	sendbuff[i++] = 0;
	x = i;
	for(j=0;i<x+strlen(pass);i++,j++){
		sendbuff[i] = pass[j];//This is password
	}
	write(sockfd, sendbuff, i);
	while((n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
		recvBuff[n] = 0;
		if(recvBuff[4] == 0x00){
			for (i = 9; i < n; i++)
				fprintf(stderr,"%c",recvBuff[i]);
			fprintf(stderr,"\n");
			return 0;
		}
		for (i = 0; i < n; i++)
		{
			//	printf("%x ",recvBuff[i]);
		}
		break;
	}
	if( n < 0) {
		return 0;
	}
	return 1;

}
int cassandra_connect(int sock,char *ip){
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
	int i,n;
	memset(recvBuff, '0' ,sizeof(recvBuff));
	sockfd=-1;
	if(sockfd < 0){
		if((sockfd = socket(AF_INET, SOCK_STREAM, 0))<0)
			return 0;
	}
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(sock);
	serv_addr.sin_addr.s_addr = inet_addr(ip);
	if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr))<0)
	{
		fprintf(stderr,"\n Error : Connect Failed \n");
<<<<<<< HEAD
		sockfd = -1;
=======
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
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
	sendbuff[4] = 0x01;

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

	while((n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0)
	{
		recvBuff[n] = 0;
		if(recvBuff[4] == 0x03)//upcode value
			AUTH_NEEDED=1;
		else if(recvBuff[4] == 0x00){
			for (i = 9; i < n; i++)
				fprintf(stderr,"%c",recvBuff[i]);
			fprintf(stderr,"\n");
			return 0;
		}
		break;
	}

	if( n < 0)
	{
		//   printf("\n Read Error \n");
	}
<<<<<<< HEAD
	/* Version */
	sendbuff[0] = 0x03; 

	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - F for AUTH_RESPONSE */
	sendbuff[4] = 0x0F;
	signed int x = strlen(user)+strlen(pass)+2+4;
	int32_to_uint8(&sendbuff[5],x);
	x = x-4;
	int32_to_uint8(&sendbuff[9],x);
	sendbuff[13]=0;
	int j;
	for(i = 14,j=0;i<14+strlen(user);i++,j++){
		sendbuff[i] = user[j];//This is username
	}
	sendbuff[i++] = 0;
	x = i;
	for(j=0;i<x+strlen(pass);i++,j++){
		sendbuff[i] = pass[j];//This is password
	}
	write(sockfd, sendbuff, i);
	while((n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
		recvBuff[n] = 0;
		if(recvBuff[4] == 0x00){
			for (i = 9; i < n; i++)
				fprintf(stderr,"%c",recvBuff[i]);
			fprintf(stderr,"\n");
			return 0;
		}
		for (i = 0; i < n; i++)
		{
			//	printf("%x ",recvBuff[i]);
		}
		break;
	}
	if( n < 0) {
		return 0;
	}
	return 1;

}

=======
	//printf("sending authentication packet\n");
	/* Create and send packet */


	return 1;

}
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
int cassandra_execute(char *query){
	int i,n;
	sendbuff[0] = 0x03; 
	/* Flag - set to 0 (default). For startup, neither compression nor tracing is required. */
	sendbuff[1] = 0x02;

	/* Stream - set to 1 (first non zero value) */
	sendbuff[2] = 0x00;
	sendbuff[3] = 0x01;

	/* Opcode - 7 for Query */
	sendbuff[4] = 0x07;
	int x = strlen(query)+4+3+4;
	/*	for(i = 0;i<4;i++){
		sendbuff[i+5]=x >> ((3-i)*8)&0xff;
		}*/
	int32_to_uint8(&sendbuff[5],x);
	x = strlen(query);
	/*for(i = 0;i<4;i++){
	  sendbuff[i+9]=x >> ((3-i)*8)&0xff;
	  }*/

	int32_to_uint8(&sendbuff[9],x);
	int j,l,m;
	for(i = 13,j=0;i<13+strlen(query);i++,j++){
		sendbuff[i] = query[j];
	}
	sendbuff[i++]=0x00;
	sendbuff[i++]=0x01;
	sendbuff[i++]=0x04;
	sendbuff[i++]=0x00;
	sendbuff[i++]=0x00;
	sendbuff[i++]=0x03;
	sendbuff[i++]=0xf2;
	write(sockfd, sendbuff, i);
	//uint8_t r[1024];
	memset(recvBuff,0,sizeof(recvBuff));
	while((n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
		recvBuff[n] = 0;
		if(recvBuff[4] == 0x00){
			for (i = 9; i < n; i++)
				fprintf(stderr,"%c",recvBuff[i]);
			fprintf(stderr,"\n");
			return 0;
		}

		for (i = 9; i < 25; i++)
		{
			//	printf("%x ", recvBuff[i]);
			//	This 16 bytes are uuid which are part of Result packet
		}
		if(uint8_to_int32(&recvBuff[i]) == 2){ // Result of SELECT Query
			i+=4;//after Check if Type of Result as above
			if(uint8_to_int32(&recvBuff[i]) == 5){//No metadata
				i+=4;//after check metadata
				COLUMN_COUNT=uint8_to_int32(&recvBuff[i]);
				i+=4;
				while(recvBuff[i] != 0)
					printf("%c ",recvBuff[i]);
				ROW_COUNT=uint8_to_int32(&recvBuff[i]);
				i+=4;
				for(l = 0;l<ROW_COUNT;l++){
					for(m = 0;m<COLUMN_COUNT;m++){
						x = uint8_to_int32(&recvBuff[i]); 
						i+=4;
						for(j = 0;j<x;j++,i++){
							if(recvBuff[i] == 1 && j == 0){
								printf("True");
							}
							if(recvBuff[i] == 0 && j == 0)
								printf("False");
							else
								printf("%c", recvBuff[i]);
						}
						printf("\t");
					}	
					printf("\n");
				}
			}
			else if(uint8_to_int32(&recvBuff[i]) == 1){
				i+=4;//after check metadata
				COLUMN_COUNT=uint8_to_int32(&recvBuff[i]);
				column_type=(int*)malloc(COLUMN_COUNT*sizeof(int));
				column_name=(char **)malloc(COLUMN_COUNT*sizeof(char*));
				printf("No of Columns are %d\n",COLUMN_COUNT);
				i+=4;
				printf("Schema and TableName\n");
				for(l = 0;l<2;l++){//First is Schema and Second is Table

					x = uint8_to_int16(&recvBuff[i]); 
					i+=2;
					for(m = 0;m<x;m++,i++)
						printf("%c", recvBuff[i]);
					printf(" ");
				}
				printf("\n");
				for(l = 0;l<COLUMN_COUNT;l++){
					x = uint8_to_int16(&recvBuff[i]); 
					i+=2;
					column_name[l]=(char *)malloc(x*sizeof(char));
					for(m = 0;m<x;m++,i++){
						column_name[l][m] = recvBuff[i];
						//	printf("%c", recvBuff[i]);
					}
					column_name[l][m] = '\0';
					j = uint8_to_int16(&recvBuff[i]); 
					i+=2;
					column_type[l] = j;
					printf(" ");
					if(j == 0){
						x = uint8_to_int16(&recvBuff[i]); 
						i+=2;
						for(m = 0;m<x;m++,i++)
							printf("%c", recvBuff[i]);
					}
					else if (j == 34){//uint8 0x22
						//	printf("Type : %x\n",j);
						j = uint8_to_int16(&recvBuff[i]); 
						i+=2;
					}
					else if (j == 33){//uint8 0x22
						//	printf("Type : %x\n",j);
						j = uint8_to_int16(&recvBuff[i]); 
						i+=2;
						//	printf("key Type : %d\n",j);
						j = uint8_to_int16(&recvBuff[i]); 
						i+=2;
						//		printf("Value Type : %d\n",j);
					}
					else {

						//		printf("Type : %x\n",j);
					}
				}
				ROW_COUNT=uint8_to_int32(&recvBuff[i]);
				i+=4;
				printf("No of Rows:%d\n",ROW_COUNT);
				for(l = 0;l<ROW_COUNT;l++){
					for(m = 0;m<COLUMN_COUNT;m++){
						x = uint8_to_int32(&recvBuff[i]); 
						i+=4;
						printf("Column Name:%s ",column_name[m]);
						if(x < 0){
							printf(" Column Value: Null");
							continue;
						}
						if(column_type[m] == 13){//VARCHAR
							char output[x];
							uint8_to_string(&recvBuff[i],x,output);
							output[x] = '\0';
							printf(" Column Value:%s",output);
							i+=x;
						}
						else if(column_type[m] == 12){//UUID
							printf("Column Value");
							for(j = 0;j<x;j++,i++){
								printf("%x", recvBuff[i]);
							}
						}
						else if(column_type[m] == 9){ // INT
							x = uint8_to_int32(&recvBuff[i]); 
							i+=4;
							printf("Column Value:%d",x);
						}
						else if(column_type[m] == 34){ //Set
							printf("Column Value");
							int p,q;
							x = uint8_to_int32(&recvBuff[i]); 
							i+=4;
							for(j = 0;j<x;j++){
								q = uint8_to_int32(&recvBuff[i]); 
								i+=4;
								for(p = 0;p<q;p++,i++){
									printf("%c", recvBuff[i]);
								}
								printf("\t");
							}

						}
						else if(column_type[m] == 33){ //map
							printf("Column Value");
							long long int p,q;
							x = uint8_to_int32(&recvBuff[i]); 
							i+=4;
							for(j = 0;j<x;j++){
								q = uint8_to_int32(&recvBuff[i]); 
								i+=4;
								for(p = 0;p<q;p++,i++){
									printf("%x", recvBuff[i]);
								}
								printf(":");
								q = uint8_to_int32(&recvBuff[i]); 
								i+=4;
								for(j = 0;j<q;j++,i++){
									printf("%x", recvBuff[i]);
								}
							}

						}
						else {
							printf("Column Value:");
							for(j = 0;j<x;j++,i++){
								printf("%d", recvBuff[i]);
							}
						}
						printf("\t");
					}	
					printf("\n");
				}
				for(i = 0;i<COLUMN_COUNT;i++)
					column_name[i];
				free(column_type);
				free(column_name);
			}
		}
		break;
	}
	if( n < 0) {
		// printf("\n Read Error \n");
		return 0;
	}
	return 1;
}
void uint8_to_string(uint8_t *in,int n,char *output){
	int i;
	for(i = 0;i<n;i++)
		output[i] = in[i];
}
