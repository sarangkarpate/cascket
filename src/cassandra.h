#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include "hashmap.h"
int AUTH_NEEDED;
int COLUMN_COUNT;
int sockfd;
int ROW_COUNT;
int *column_type;
char **column_name;

uint8_t recvBuff[102400000], sendbuff[1024000];
struct sockaddr_in serv_addr;
typedef struct column_desc{
	char *name;
	uint16_t type; 	
}column_desc;
typedef struct result_set{
	Hashmap *map;
	uint8_t *buffer;
	int column_count, row_count, curr_pos;
	column_desc *col_desc_array;	
}result_set;
void int32_to_uint8(uint8_t*,int);
int uint8_to_int32(uint8_t*);
int uint8_to_int16(uint8_t*);
void  uint8_to_string(uint8_t*, int,char * );
int cassandra_connect(int,char*,char*,char*);
int cassandra_execute(char*);
int cassandra_authenticate(char*,char*);
#define ASCII		0x0001
#define BIGINT		0x0002
#define BLOB		0x0003
#define BOOLEAN		0x0004
#define COUNTER		0x0005
#define DECIMAL		0x0006
#define DOUBLE		0x0007
#define FLOAT		0x0008
#define INT		0x0009
#define TIMESTAMP	0x000B
#define UUID		0x000C
#define VARCHAR		0x000D
#define VARINT		0x000E
#define TIMEUUID	0x000F
#define INET		0x0010
#define LIST 		0x0020
#define MAP 		0x0021
#define SET 		0x0022
#define UDT 		0x0030
#define TUPLE 		0x0031

