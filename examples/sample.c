#include<stdio.h>
#include "../src/cassandra.h"
int main(void)
{
	char str[1024];
	if(cassandra_connect(9042,"127.0.0.1","javed","javed"))
		printf("Connected to Database\n");
	else 
		printf("Error in connecting the database\n");
	/* Create and send packet */
	while(1){
		gets(str);
		if(cassandra_execute(str))
			printf("query sent\n");
		else
			printf("Error in executing query\n");
	}
	return 0;
}
