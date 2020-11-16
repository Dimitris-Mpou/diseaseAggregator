#ifndef FUNCTIONS_H
#define FUNCTIONS_H

    // input functions
void countInput(char[256], int *);
int compareDates(char[11], char[11]);
int input(char[256], dataHT **, int, char[11]);
int commandIdentifier(char[256], Record *);
int update(char[256], dataHT **, int, Hashtable **, int, int, char[11]);

    // dataHT functions
void dataHashtableInit(dataHT **, int);
int Hashfunction(char[32], int);
int dataHashtableInsert(dataHT **, int, Record, int, char[11], Record **);
void dataHashtableDelete(dataHT **, int);

    // disease and country HT functions
void HashtableInit(Hashtable **, int, int);
int HashtableInsert(Hashtable **, int , int , Record *, char[32], char);
int HashtableCount(Hashtable **, Record, int, int);
int HashtableCountExit(Hashtable **, Record, int, int);
void hashtableDelete(Hashtable **, int, int);

    // AVL Tree functions
void AVLinit(avlTree *);
int AVLinsert(avlTree **, Record *);
void AVLrotation(avlTree **, char[11]);
void AVLprint(avlTree *);
int AVLcount(avlTree *);
int AVLcountDates(avlTree *, char[11], char[11]);
int AVLcountDatesExit(avlTree *, char[11], char[11]);
void AVLcountDatesAge(avlTree *, char[11], int *);
void AVLdelete(avlTree *);
void AVLinsertHeapDates(avlTree *, Heap *, char[11], char[11]);
int AVLcountPatients(avlTree *);

    // heap and queueHeap functions
void queueInsert(heapQueue *, Heap *);
Heap * queuePop(heapQueue **);
void queueEmpty(heapQueue *);
void heapSwap(Heap *, Heap *);
void heapInit(Heap *);
void heapInsert(Heap *, int);
void heapify(Heap *);
void heapPrint(Heap *);
int heapPop(Heap *, int *);
void heapDelete(Heap *);

void writeProtocol(int, char *, int);
void readProtocol(int, char *, int);
void summaryStatistics(Hashtable **, int, int, int, int, int, char **, char[32]);
void readStats(int, int);

void handlerIntQuit(int);
void handlerChld(int);
void handlerUsr1(int);

  //  queries
void diseaseFrequency(Hashtable ***, Record, int, int, int, char *);
void diseaseFrequencyCountry(Hashtable ***, Record, int, int, int, char *, char **);
void topkAgeRanges(Hashtable ***, Record , int, int, int, char *, char **, int, int , int *);
void searchPatientRecord(dataHT ***, Record, int, int, char *, int, int, int *, int *);
void numPatient(Hashtable ***, Record , int, int , int, char *, char **);
void numPatientCountry(Hashtable ***, Record , int, int, int, int, int, int *, char *, char **);

#endif
