#include<stdio.h>
#include <stdlib.h>
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

	str = "select * from demodb.users;";

	result_set *rs;
	rs = cass_execute(str);

	printf("Normal query\n");

	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "user");
		printf("User is %s\n", temp);

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

	//str = "insert into demodb.test (name, age, surname ) VALUES ( 'abhishek', 21, 'joshi')";
	str = "select * from demodb.users;";

	cps = cass_prepare_statement(str);

	rs = cass_execute_prepared_statement(cps);

	printf("Prepared Statement\n");
	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "user");
		printf("User is %s\n", temp);
/*
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
		*/
		printf("\n");
	}


	//printf("Got cps.id as %d\n", cps->id);
	cass_batch *b;
	init_batch(b);
	add_to_batch_simple(b,"INSERT INTO demodb.users (user, gender ) VALUES ( 'j','m')");
add_to_batch_simple(b,"INSERT INTO demodb.users (user, gender ) VALUES ( 'r','m')");
	int i;
//	for(i = 0;i<b->curr_pos;i++){
///		printf("%d ",b->queries[i]);
//	}
	cass_execute_batch(b);
	result_set_destroy(rs);
	return 0;
}
