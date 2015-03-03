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

	str = "select * from demodb.test;";

	result_set *rs;
	rs = cass_execute(str);

	printf("Normal query\n");

	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "name");
		printf("Name is %s\n", temp);

		temp = (char  *)get_val(rs, "surname");
		printf("Surname is %s\n", temp);

		age = (int *)get_val(rs, "age");
		printf("Age is %d\n", *age);
		
		printf("\n");
	}

	//str = "insert into demodb.test (name, age, surname ) VALUES ( 'abhishek', 21, 'joshi')";
	str = "select * from demodb.test;";

	cps = cass_prepare_statement(str);

	rs = cass_execute_prepared_statement(cps);

	printf("Prepared Statement\n");
	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "name");
		printf("Name is %s\n", temp);

		temp = (char  *)get_val(rs, "surname");
		printf("Surname is %s\n", temp);

		age = (int *)get_val(rs, "age");
		printf("Age is %d\n", *age);
		
		printf("\n");
	}


	//printf("Got cps.id as %d\n", cps->id);

	result_set_destroy(rs);
	return 0;
}
