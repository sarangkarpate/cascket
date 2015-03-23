#include<stdio.h>
#include <stdlib.h>
#include<string.h>
#include "cassandra.h"
#include "cfuhash.h"
int main(void)
{
	char *str;
	char *temp;
	int *age;
	cass_prepared_statement *cps;

	if(cass_connect(9042,"127.0.0.1","cassandra","cassandra"))
		printf("Connected to Database\n");
	else 
		printf("Error in connecting the database\n");

	str = "select * from system.local";

	result_set *rs;
	rs = cass_execute(str);
	cass_map *m;
	cass_set *s;
	int c;
	void *ab;
	printf("Normal query\n");

	int i;
	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "rack");
		printf("User is %s\n", temp);
		m = (cass_map *)get_val(rs,"truncated_at");
		if(m){
			printf("key is ");
		//	ab = get_val_map(m,"55080ab05d9c388690a4acb25fe1f77b");
			printf("%x\n",m->value_type);
		}
		s = (cass_set*)get_val(rs,"tokens");
		if(s){
			for(i = 0;i<s->length;i++)
				printf("%s\n",(char*)s->set[i]);
		}
		/*temp = (char  *)get_val(rs, "password");
		if(temp)
			printf("password is %s\n", temp);
		else
			printf("Password is null\n");

		age = (int *)get_val(rs, "number");
		if(age)
			printf("number is %d\n", *age);
		else
			printf("number is null\n");
*/		
		printf("\n");
	}
/*	//str = "insert into demodb.test (name, age, surname ) VALUES ( 'abhishek', 21, 'joshi')";
	str = "select * from system.local;";

	cps = cass_prepare_statement(str);

	rs = cass_execute_prepared_statement(cps);

	printf("Prepared Statement\n");
	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "rack");
		printf("User is %s\n", temp);

		temp = (char  *)get_val(rs, "password");
		if(temp)
			printf("password is %s\n", temp);
		else
			printf("Password is null\n");

		age = (int *)get_val(rs, "number");
		if(age)
			printf("number is %d\n", *age);
		else
			printf("number is null\n");
		
		printf("\n");
	}
*/

	//printf("Got cps.id as %d\n", cps->id);
/*	cass_batch *b = create_batch();
	init_batch(b);
	str = "INSERT INTO demodb.users (user, gender) VALUES ( 'j', 'm' )";
	cps = cass_prepare_statement(str);
//	add_to_batch_simple(b,"INSERT INTO demodb.users (user, gender ) VALUES ( 'j','m')");
add_to_batch_simple(b,"INSERT INTO demodb.users (user, gender ) VALUES ( 'r','m')");
	add_to_batch_prepared(b,cps);
	int i;
//	for(i = 0;i<b->curr_pos;i++){
///		printf("%d ",b->queries[i]);
//	}
	cass_execute_batch(b);*/
	result_set_destroy(rs);
	return 0;
}
