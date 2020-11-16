OBJS = main.o input.o hashtables.o heap.o avlTree.o protocol.o countStats.o queries.o
SOURCE = main.c input.c hashtables.c heap.c avlTree.c protocol.c countStats.c	queries.c
HEADER = structs.h functions.h
OUT = diseaseAggregator
CC = gcc
FLAG = -c

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT)

main.o: main.c
	$(CC) $(FLAG) main.c

input.o: input.c
	$(CC) $(FLAG) input.c

hashtables.o: hashtables.c
	$(CC) $(FLAG) hashtables.c

avlTree.o: avlTree.c
	$(CC) $(FLAG) avlTree.c

heap.o: heap.c
	$(CC) $(FLAG) heap.c

protocol.o: protocol.c
	$(CC) $(FLAG) protocol.c

countStats.o: countStats.c
	$(CC) $(FLAG) countStats.c

queries.o: queries.c
	$(CC) $(FLAG) queries.c

worker:
	gcc -o worker worker.c input.c hashtables.c avlTree.c heap.c protocol.c countStats.c queries.c

clean:
	rm -f $(OBJS) $(OUT)
