#include<stdio.h>
#include <stdlib.h>
#include "cassandra.h"
#include "cfuhash.h"
int main(void)
{
	char *str;
	char *temp;

	if(cassandra_connect(9042,"127.0.0.1","cassandra","cassandra"))
		printf("Connected to Database\n");
	else 
		printf("Error in connecting the database\n");

	/* Create and send packet */
	str = "select * from demodb.test;";
	//gets(str);
	result_set *rs;
	rs = cassandra_execute(str);
	while (has_next(rs))
	{
		temp = (char *)get_val(rs, "name");
		printf("got value as %s\n", temp);
		temp = (char  *)get_val(rs, "surname");
		printf("surname value as %s\n", temp);
	}
	free(rs);
	return 0;
}
