csvdb: main.o query.o db.o
	gcc -o csvdb main.o db.o query.o

main.o: main.c query.h
	gcc -c -o main.o main.c

db.o: db.c db.h
	gcc -c -o db.o db.c

query.o: query.c query.h db.h
	gcc -c -o query.o query.c
