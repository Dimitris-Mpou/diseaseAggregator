#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void dataHashtableInit(dataHT **table, int size){   // Kanw initialize ton dataHashtable
  int i;
  for (i = 0; i < size; i++) {
    table[i]->next = NULL;
  }
}

int Hashfunction(char key[32], int size){  // Hashfunction gia olous tous Hashtables
  int i, sum;
  unsigned int hash;

    // Kwdikas apo Wikipedia (Jenkins hash function) me link: https://en.wikipedia.org/wiki/Jenkins_hash_function
	hash = 0;
	i = 0;
  while(key[i] != '\0'){
    hash += key[i];
	  hash += (hash << 10);
    hash ^= (hash >> 6);
    i++;
  }
	hash += (hash << 3);
	hash ^= (hash >> 11);
	hash += (hash << 15);
  return hash % size;
}

int dataHashtableInsert(dataHT **table, int size, Record record, int bool, char date[11], Record **recAddress){   // Kanei insert eggrafis sto dataHashtable
  dataHT *cur;

  cur = table[Hashfunction(record.recordID, size)];
  while(cur->next != NULL){
    if(strcmp(record.recordID, cur->record.recordID) == 0){
      if(bool){
        return 0;     // Exei ginei idi ENTER
      }else{          // Tote einai eggrafi EXIT kai tsekarw egkurotita imerominias
        if(compareDates(date, cur->record.entryDate)==1 ){
          return 0;
        }else{
          strcpy(cur->record.exitDate, date);
          return 0;
        }
      }
    }
    cur = cur->next;
  }
  if(!bool){
//    printf("EXIT without been ENTER, ignoring: %s\n", record.recordID);
    return 0;
  }
  if(recAddress!=NULL)
    *recAddress = &(cur->record);
//  printf("Insert record: %s\n", record.recordID);
  strcpy(cur->record.recordID, record.recordID);
  strcpy(cur->record.patientFirstName, record.patientFirstName);
  strcpy(cur->record.patientLastName, record.patientLastName);
  strcpy(cur->record.diseaseID, record.diseaseID);
  strcpy(cur->record.country, record.country);
  strcpy(cur->record.entryDate, date);
  strcpy(cur->record.exitDate, "--");
  cur->record.age = record.age;
  cur->next = malloc(sizeof(dataHT));
  cur->next->next = NULL;

  return 0;
}

void dataHashtableDelete(dataHT **table, int size){         // Kanei diagrafi tou dataHashtable
  int i;
  dataHT *cur, *next;

  for (i = 0; i < size; i++) {
    cur = table[i];
    while(cur->next != NULL){
      next = cur->next;
      free(cur);
      cur = next;
    }
  }
  free(table);
}

void HashtableInit(Hashtable **table, int size, int bucketEntryCapacity){    // Kanw initialize tous diseaseHashtable kai countryHashtable
  int i;
  for (i = 0; i < size; i++) {
    table[i]->next = NULL;
    table[i]->entries = malloc(bucketEntryCapacity*sizeof(bucketEntry));
    table[i]->entries[0].name = NULL;   // Paradoxi gia na kserw posa entries exei o trexwn bucket
    table[i]->entries[0].tree = NULL;
  }
}

int HashtableInsert(Hashtable **table, int size, int bucketEntryCapacity, Record *record, char key[32], char id){   // Kanei insert eggrafis sto dataHashtable
  int i, flag;
  Hashtable *cur;

  flag = 0;
  cur = table[Hashfunction(key, size)];
  while(cur->next != NULL){
    flag = 0;   // An isxuei tote exei ginei i eisagwgi
    i = 0;
    while(i < bucketEntryCapacity){
      if(cur->entries[i].name == NULL){     ///////// Edit
        break;
      }
      if(strcmp(cur->entries[i].name, key) == 0){
        AVLinsert(&(cur->entries[i].tree), record);    // Eisagwgi sto antistoixo dentro
        flag = 1;
        break;
      }
      i++;
    }
    if(flag){
      break;
    }else if(cur->entries[i].name == NULL && i < bucketEntryCapacity){
      cur->entries[i].tree = malloc(sizeof(avlTree));    // Dimiourgia dentrou me 1i eisagwgi
      AVLinit(cur->entries[i].tree);
      AVLinsert(&(cur->entries[i].tree), record);
      if(id == 'd'){
        cur->entries[i].name = record->diseaseID;
      }else{
        cur->entries[i].name = record->country;
      }
      if (i+1 < bucketEntryCapacity) {
        cur->entries[i+1].name = NULL;
        cur->entries[i+1].tree = NULL;
      }
      flag = 1;
      break;
    }
    cur = cur->next;
  }
  if(!flag){          // An den egine eisagwgi
    cur->entries[0].tree = malloc(sizeof(avlTree));      // 1i eisagwgi autou tou bucket
    AVLinit(cur->entries[0].tree);
    AVLinsert(&(cur->entries[0].tree), record);
    if(bucketEntryCapacity > 0){
      cur->entries[1].name = NULL;
      cur->entries[1].tree = NULL;
    }
    if(id == 'd'){
      cur->entries[0].name = record->diseaseID;
    }else{
      cur->entries[0].name = record->country;
    }
    cur->next = malloc(sizeof(Hashtable));
    cur->next->next = NULL;
    cur->next->entries = malloc(bucketEntryCapacity*sizeof(bucketEntry));
    cur->next->entries[0].name = NULL;
    cur->next->entries[0].tree = NULL;
  }

  return 0;
}

int HashtableCount(Hashtable **table, Record recData, int size, int bucketEntryCapacity){   // sunartisi gia tin diseaseFrequency
  int i, j, count;
  Hashtable *cur;

  count = 0;

  cur = table[Hashfunction(recData.diseaseID, size)];
  while(cur->next != NULL){
    i = 0;
    while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
      if(strcmp(cur->entries[i].name, recData.diseaseID) == 0){
        count = AVLcountDates(cur->entries[i].tree, recData.entryDate, recData.exitDate);    // Metrisi dentrou
        return count;
      }
      i++;
    }
    cur = cur->next;
  }

  return count;
}

int HashtableCountExit(Hashtable **table, Record recData, int size, int bucketEntryCapacity){   // sunartisi gia tin diseaseFrequency
  int i, j, count;
  Hashtable *cur;

  count = 0;

  cur = table[Hashfunction(recData.diseaseID, size)];
  while(cur->next != NULL){
    i = 0;
    while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
      if(strcmp(cur->entries[i].name, recData.diseaseID) == 0){
        count = AVLcountDatesExit(cur->entries[i].tree, recData.entryDate, recData.exitDate);    // Metrisi dentrou
        return count;
      }
      i++;
    }
    cur = cur->next;
  }

  return count;
}

void hashtableDelete(Hashtable **table, int size, int bucketEntryCapacity){
  int i, j, flag;
  Hashtable *cur, *next;

  for(j=0; j<size; j++){
    flag = 0;
    cur = table[j];
    while(cur != NULL){
      i = 0;
      while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
        AVLdelete(cur->entries[i].tree);
        i++;
      }
      next = cur->next;
      free(cur);
      cur = next;
    }
  }
}
