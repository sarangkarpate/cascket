#include<stdio.h>
#include "../src/cassandra.h"
int main(void)
{
	char str[1024];
<<<<<<< HEAD
	if(cassandra_connect(9042,"127.0.0.1","javed","javed"))
=======
	if(cassandra_connect(9042,"127.0.0.1"))
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
		printf("Connected to Database\n");
	else 
		printf("Error in connecting the database\n");
	/* Create and send packet */
<<<<<<< HEAD
=======
	if(cassandra_authenticate("javed","javed"))
		printf("authenticated successfully\n");
	else
		printf("Error in Authenticating\n");
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
	while(1){
		gets(str);
		if(cassandra_execute(str))
			printf("query sent\n");
		else
			printf("Error in executing query\n");
	}
	return 0;
}
