try : cassandra.o sample.o
	gcc cassandra.o sample.o -o try
sample.o: sample.c
	gcc -c sample.c -o sample.o
cassandra.o:cassandra.c
	gcc -c cassandra.c -o cassandra.o
clean:
	rm -rf *.o try
