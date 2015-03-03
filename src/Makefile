try: cfu.o cfuhash.o cfustring.o sample.o cassandra.o
	cc cfu.o cfuhash.o cfustring.o sample.o cassandra.o -o try
cfu.o: cfu.c
	cc -g -c -Wall -Wextra cfu.c -o cfu.o
sample.o: sample.c
	cc -g -c -Wall -Wextra sample.c -o sample.o
cfuhash.o: cfuhash.c
	cc -g -c -Wall -Wextra cfuhash.c -o cfuhash.o
cassandra.o: cassandra.c cfuhash.o
	cc -g -c -Wall -Wextra cassandra.c -o cassandra.o
cfustring.o: cfustring.c
	cc -g -c -Wall -Wextra cfustring.c -o cfustring.o
hash_usage.o: hash_usage.c
	cc -g -c -Wall -Wextra hash_usage.c -o hash_usage.o
clean:
	rm *.o try
