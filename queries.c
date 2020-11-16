#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void diseaseFrequency(Hashtable ***disease, Record recData, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, int myCountriesCount, char *message){
  int i, j, count;

  i = 2;
  j = 0;
  while(message[i] != ' '){                       //  Antigrafw ta dedomena
    recData.diseaseID[j] = message[i];
    i++;
    j++;
  }
  recData.diseaseID[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.entryDate[j] = message[i];
    i++;
  }
  recData.entryDate[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.exitDate[j] = message[i];
    i++;
  }
  recData.exitDate[j] = '\0';
  count = 0;
  for(i=0; i<myCountriesCount; i++)
    count += HashtableCount(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
  sprintf(message, "%d", count);
}

void diseaseFrequencyCountry(Hashtable ***disease, Record recData, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, int myCountriesCount, char *message, char **myCountries){
  int i, j, count;

  i = 2;
  j = 0;
  while(message[i] != ' '){                       //  Antigrafw ta dedomena
    recData.diseaseID[j] = message[i];
    i++;
    j++;
  }
  recData.diseaseID[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.entryDate[j] = message[i];
    i++;
  }
  recData.entryDate[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.exitDate[j] = message[i];
    i++;
  }
  recData.exitDate[j] = '\0';
  i++;
  j = 0;
  while(message[i] != '\0'){
    recData.country[j] = message[i];
    i++;
    j++;
  }
  recData.country[j] = '\0';
  count = 0;
  for(i=0; i<myCountriesCount; i++){
    if(strcmp(myCountries[i], recData.country)==0){
      count += HashtableCount(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
    }
  }
  sprintf(message, "%d", count);
}

void topkAgeRanges(Hashtable ***disease, Record recData, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, int myCountriesCount, char *message, char **myCountries, int fdWrite, int bufferSize, int *success){
  int i, j, z, age, ages[4], sequence[4];
  char path[256];
  Hashtable *curr;
  Heap *heap;

  i = 2;
  j = 0;
  while(message[i] != ' '){                       //  Antigrafw ta dedomena
    recData.recordID[j] = message[i];
    i++;
    j++;
  }
  recData.recordID[j] = '\0';
  i++;
  j = 0;
  while(message[i] != ' '){
    recData.country[j] = message[i];
    i++;
    j++;
  }
  recData.country[j] = '\0';
  i++;
  j = 0;
  while(message[i] != ' '){
    recData.diseaseID[j] = message[i];
    i++;
    j++;
  }
  recData.diseaseID[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.entryDate[j] = message[i];
    i++;
  }
  recData.entryDate[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.exitDate[j] = message[i];
    i++;
  }
  recData.exitDate[j] = '\0';
  for(i=0; i<myCountriesCount; i++){
    if( strcmp(myCountries[i], recData.country) == 0 ){
      heap = malloc(sizeof(Heap));
      heapInit(heap);
      curr = disease[i][Hashfunction(recData.diseaseID, diseaseHashtableNumOfEntries)];
      while(curr->next != NULL){
        j = 0;
        while(j< bucketEntryCapacity && curr->entries[j].name != NULL){
          if(strcmp(recData.diseaseID, curr->entries[j].name) == 0){
            AVLinsertHeapDates(curr->entries[j].tree, heap, recData.entryDate, recData.exitDate);
            break;
          }
          j++;
        }
        curr = curr->next;
      }
      heapify(heap);
      for(j=0; j<4; j++){
        ages[j] = 0;
        sequence[j] = -1;
      }
      for(j=0; j<4; j++){
        z = heapPop(heap, &age);
        if(z != -2){
          if( j<atoi(recData.recordID) )
            sequence[j] = age;
          if(age==0)
            ages[0] = z;
          else if(age==1)
            ages[1] = z;
          else if(age==2)
            ages[2] = z;
          else if(age==3)
            ages[3] = z;
        }
      }
      z = ages[0] + ages[1] + ages[2] + ages[3];
      j = 0;
      strcpy(message, "");
      while(j<4){
        if(sequence[j]== -1)
          break;
        if(sequence[j]==0){
          sprintf(path, "0-20: %d%%\n", 100*ages[0]/z);
          strcat(message, path);
        }else if(sequence[j]==1){
          sprintf(path, "21-40: %d%%\n", 100*ages[1]/z);
          strcat(message, path);
        }else if(sequence[j]==2){
          sprintf(path, "41-60: %d%%\n", 100*ages[2]/z);
          strcat(message, path);
        }else if(sequence[j]==3){
          sprintf(path, "60+: %d%%\n", 100*ages[3]/z);
          strcat(message, path);
        }
        j++;
      }
      heapDelete(heap);
      writeProtocol(fdWrite, message, bufferSize);
      (*success)++;
    }
  }
}

void searchPatientRecord(dataHT ***records, Record recData, int dataHashtableSize, int myCountriesCount, char *message, int fdWrite, int bufferSize, int *success, int *fail){
  int i, j, flag;
  dataHT *cur;

  i = 2;
  j = 0;
  while(message[i] != '\0'){
    recData.recordID[j] = message[i];
    i++;
    j++;
  }
  recData.recordID[j] = '\0';
  flag = 0;
  for(i=0; i<myCountriesCount; i++){
    cur = records[i][Hashfunction(recData.recordID, dataHashtableSize)];
    while(cur->next != NULL){
      if(strcmp(recData.recordID, cur->record.recordID) == 0){
        sprintf(message, "%s %s %s %s %d %s %s", cur->record.recordID, cur->record.patientFirstName, cur->record.patientLastName, cur->record.diseaseID, cur->record.age, cur->record.entryDate, cur->record.exitDate);
        writeProtocol(fdWrite, message, bufferSize);
        flag = 1;
        *(success)++;
        break;
      }
      cur = cur->next;
    }
    if(flag)
      break;
  }
  if(!flag){
    strcpy(message, "Not Found");
    writeProtocol(fdWrite, message, bufferSize);
    *(fail)++;
  }
}

void numPatient(Hashtable ***disease, Record recData, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, int myCountriesCount, char *message, char **myCountries){
  int i, j, flag, count;
  char path[256];

  flag = 0;
  if(message[0] == '5')
    flag = 1;
  i = 2;
  j = 0;
  while(message[i] != ' '){                       //  Antigrafw ta dedomena
    recData.diseaseID[j] = message[i];
    i++;
    j++;
  }
  recData.diseaseID[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.entryDate[j] = message[i];
    i++;
  }
  recData.entryDate[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.exitDate[j] = message[i];
    i++;
  }
  recData.exitDate[j] = '\0';
  strcpy(message, "");
  for(i=0; i<myCountriesCount; i++){
    if(flag)
      count = HashtableCount(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
    else
      count = HashtableCountExit(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
    sprintf(path, "%s %d_", myCountries[i], count);
    strcat(message, path);
  }
  strcat(message, "^");
}

void numPatientCountry(Hashtable ***disease, Record recData, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, int myCountriesCount, int fdWrite, int bufferSize, int *success, char *message, char **myCountries){
  int i, j, count;

  i = 2;
  j = 0;
  while(message[i] != ' '){                       //  Antigrafw ta dedomena
    recData.diseaseID[j] = message[i];
    i++;
    j++;
  }
  recData.diseaseID[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.entryDate[j] = message[i];
    i++;
  }
  recData.entryDate[j] = '\0';
  i++;
  for(j=0; j<10; j++){
    recData.exitDate[j] = message[i];
    i++;
  }
  recData.exitDate[j] = '\0';
  i++;
  j = 0;
  while(message[i] != '\0'){                       //  Antigrafw ta dedomena
    recData.country[j] = message[i];
    i++;
    j++;
  }
  recData.country[j] = '\0';
  for(i=0; i<myCountriesCount; i++){
    if(strcmp(myCountries[i], recData.country)==0){
      if(message[0] == '6')
        count = HashtableCount(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
      else
        count = HashtableCountExit(disease[i], recData, diseaseHashtableNumOfEntries, bucketEntryCapacity);
      sprintf(message, "%s %d", myCountries[i], count);
      writeProtocol(fdWrite, message, bufferSize);
      *(success)++;
      break;
    }
  }
}
