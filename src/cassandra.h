#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include "cfuhash.h"

#define CUSTOM		0x0000 /* The custom value is a [string] */
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


#define ERROR		0x00
#define STARTUP		0x01
#define READY		0x02
#define AUTHENTICATE	0x03
#define OPTIONS		0x05
#define SUPPORTED	0x06
#define QUERY		0x07
#define RESULT		0x08
#define PREPARE		0x09
#define EXECUTE		0x0A
#define REGISTER	0x0B
#define EVENT		0x0C
#define BATCH		0x0D
#define AUTH_CHALLENGE	0x0E
#define AUTH_RESPONSE	0x0F
#define AUTH_SUCCESS	0x10

#define VOID		0x0001/*: for results carrying no information.*/
#define ROWS		0x0002/*: for results to select queries, returning a set of rows.*/
#define SET_KEYSPACE	0x0003/*: the result to a `use` query.*/
#define PREPARED	0x0004/*: result to a PREPARE message.*/
#define SCHEMA_CHANGE	0x0005/*: the result to a schema altering query.*/


int AUTH_NEEDED;
int COLUMN_COUNT;
int sockfd;
int ROW_COUNT;
struct sockaddr_in serv_addr;

typedef struct column_desc{
	char *name;				/* Column description structure for ResultSet */
	uint16_t type; 	
	/* For future reference, may need to add another field for CUSTOM data type name */
}column_desc;

typedef struct result_set{
	cfuhash_table_t *map;
	uint8_t *buffer;			/* ResultSet Object description */
	int column_count, row_count, curr_pos, curr_row;
	column_desc *col_desc_array;
	char *table_name;
	char *schema_name;	
}result_set;

typedef struct cass_prepared_statement
{
	uint8_t *id;
	int id_length;
} cass_prepared_statement;
typedef struct cass_batch 
{
	char queries[102400];
	int curr_pos;
	short count;
}cass_batch;
typedef struct cass_map 
{
	cfuhash_table_t *map;
}cass_map;
/* Helping functions */
void int32_to_uint8(uint8_t *, int);
int uint8_to_int32(uint8_t *);
int uint8_to_int16(uint8_t *);
void uint8_to_string(uint8_t *, int, char *);

/* Following are the functions for Resultset */
void init_rs(result_set *);
result_set *create_rs();
int has_next(result_set *);
void *get_val(result_set *rs, char *column_name);
void result_set_destroy(result_set *rs);

/* Cassandra functions */
result_set *cass_execute(char *);
int cass_connect(int, char *, char *, char *);

cass_prepared_statement *cass_prepare_statement(char *);
result_set *cass_execute_prepared_statement(cass_prepared_statement *);

/* Batch Functions */
void add_to_batch_simple(cass_batch *,char *);
void add_to_batch_prepared(cass_batch*,cass_prepared_statement*);
int cass_execute_batch(cass_batch *);
void init_batch(cass_batch *);
cass_batch *create_batch();

