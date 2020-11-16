#ifndef STRUCTS_H
#define STRUCTS_H

typedef struct Records{                          // Domi eggrafwn
  char recordID[32];
  char patientFirstName[32];
  char patientLastName[32];
  char diseaseID[32];
  char country[32];
  char entryDate[11];
  char exitDate[11];
  int age;
}Record;

typedef struct dataHashtableNode{                // Domi dataHashtable
  struct dataHashtableNode *next;
  Record record;
}dataHT;

typedef struct avlTreeList{                // Domi avlList
  struct avlTreeList *next;
  Record *rec;
}avlList;

typedef struct avlTreeNode{                    // Domi AVL
  struct avlTreeNode *left;
  struct avlTreeNode *right;
  avlList *equal;
  Record *rec;
  int depth;
}avlTree;

typedef struct bucketEntries{                     // Domi bucketEntry
  char *name;
  avlTree *tree;
}bucketEntry;

typedef struct diseaseHashtableNode{            // Domi diseaseHashtable kai countryHashtable
  struct diseaseHashtableNode *next;
  bucketEntry *entries;
}Hashtable;

typedef struct heapNode{                         // Domi Heap
  struct heapNode *left;
  struct heapNode *right;
  int ageRange;
  int count;
}Heap;

typedef struct heapQueueNode{                    // Domi Heap Queue
  struct heapQueueNode *next;
  Heap *heap;
}heapQueue;

#endif
