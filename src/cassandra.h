#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
int AUTH_NEEDED;
int COLUMN_COUNT;
int sockfd;
int ROW_COUNT;
int *column_type;
char **column_name;
uint8_t recvBuff[102400000], sendbuff[1024000];
struct sockaddr_in serv_addr;
void int32_to_uint8(uint8_t*,int);
int uint8_to_int32(uint8_t*);
int uint8_to_int16(uint8_t*);
void  uint8_to_string(uint8_t*, int,char * );
<<<<<<< HEAD
int cassandra_connect(int,char*,char*,char*);
=======
int cassandra_connect(int,char*);
>>>>>>> 0bba89deac50a9cf9dfc4665591f52c4bf12dba6
int cassandra_execute(char*);
int cassandra_authenticate(char*,char*);
