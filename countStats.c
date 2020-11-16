#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void summaryStatistics(Hashtable **table, int size, int bucketEntryCapacity, int fileCount, int fdWrite, int bufferSize, char**files, char country[32]){
  int i, j, z, k, count, ***ageCount;
  char **diseases, message[256];
  Hashtable *cur;

  count = 0;
  for(j=0; j<size; j++){
    cur = table[j];
    while(cur->next != NULL){
      i = 0;
      while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
        count++;
        i++;
      }
      cur = cur->next;
    }
  }
//  printf("%s has %d diseases\n", country, count);
  diseases = malloc(count*sizeof(char *));

  ageCount = malloc(fileCount*sizeof(int **));
  for(i=0; i<fileCount; i++){
    ageCount[i] = malloc(count*sizeof(int *));
    for(j=0; j<count; j++){
      ageCount[i][j] = malloc(4*sizeof(int));
    }
  }
  for(i=0; i<fileCount; i++){
    for(j=0; j<count; j++){
      for(z=0; z<4; z++){
        ageCount[i][j][z] = 0;
      }
    }
  }

  for(i=0; i<count; i++)
    diseases[i] = malloc(32*sizeof(char));
  count = 0;
  for(j=0; j<size; j++){
    cur = table[j];
    while(cur->next != NULL){
      i = 0;
      while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
        strcpy(diseases[count], cur->entries[i].name);
        count++;
        i++;
      }
      cur = cur->next;
    }
  }
  for(z=0; z<fileCount; z++){
    for(k=0; k<count; k++){
      cur = table[Hashfunction(diseases[k], size)];
      while(cur->next != NULL){
        i = 0;
        while(i < bucketEntryCapacity && cur->entries[i].name != NULL){
          if(strcmp(cur->entries[i].name, diseases[k]) == 0){
            AVLcountDatesAge(cur->entries[i].tree, files[z], ageCount[z][k]);    // Metrisi dentrou
          }
          i++;
        }
        cur = cur->next;
      }
    }
  }
  sprintf(message, "c %s", country);
  writeProtocol(fdWrite, message, bufferSize);
  for(z=0; z<fileCount; z++){
    sprintf(message, "f %s", files[z]);
    writeProtocol(fdWrite, message, bufferSize);
    for(k=0; k<count; k++){
//      printf("%s, %s, %s: %d %d %d %d\n", files[z], country, diseases[k], ageCount[z][k][0], ageCount[z][k][1], ageCount[z][k][2], ageCount[z][k][3]);
      sprintf(message, "d %s %d %d %d %d", diseases[k], ageCount[z][k][0], ageCount[z][k][1], ageCount[z][k][2], ageCount[z][k][3]);
      writeProtocol(fdWrite, message, bufferSize);
    }
  }

  for(i=0; i<count; i++)
    free(diseases[i]);
  free(diseases);
}

void readStats(int fdRead, int bufferSize){
  int i, j, age[4];
  char message[256], country[32], disease[32], date[11], tempCount[6];

  readProtocol(fdRead, message, bufferSize);
//  printf("\t\t\tC or E %s\n", message);
  while(message[0]=='c'){
    i = 2;
    j = 0;
    while(message[i]!='\0'){       // Diabazw tin country
      country[j] = message[i];
      i++;
      j++;
    }
    country[j] = '\0';
    readProtocol(fdRead, message, bufferSize);
//    printf("\t\t\tF %s\n", message);
    while(message[0]=='f'){
      i = 2;
      j = 0;
      while(message[i]!='\0'){       // Diabazw tin date
        date[j] = message[i];
        i++;
        j++;
      }
      date[j] = '\0';
      printf("%s\n%s\n", date, country);
      readProtocol(fdRead, message, bufferSize);
//      printf("\t\t\tD %s\n", message);
      while(message[0]=='d'){
        i = 2;
        j = 0;
        while(message[i]!=' '){       // Diabazw to disease
          disease[j] = message[i];
          i++;
          j++;
        }
        disease[j] = '\0';
        i++;
        j = 0;
        while(message[i]!=' '){       // Age[0]
          tempCount[j] = message[i];
          i++;
          j++;
        }
        tempCount[j] = '\0';
        age[0] = atoi(tempCount);
        i++;
        j = 0;
        while(message[i]!=' '){       // Age[1]
          tempCount[j] = message[i];
          i++;
          j++;
        }
        tempCount[j] = '\0';
        age[1] = atoi(tempCount);
        i++;
        j = 0;
        while(message[i]!=' '){       // Age[2]
          tempCount[j] = message[i];
          i++;
          j++;
        }
        tempCount[j] = '\0';
        age[2] = atoi(tempCount);
        i++;
        j = 0;
        while(message[i]!='\0'){       // Age[3]
          tempCount[j] = message[i];
          i++;
          j++;
        }
        tempCount[j] = '\0';
        age[3] = atoi(tempCount);
        i++;
        j = 0;
        if(age[0] || age[1] || age[2] || age[3]){
          printf("%s\nAge range 0-20: %d cases\nAge range 21-40: %d cases\nAge range 41-60: %d cases\nAge range 60+: %d cases\n\n", disease, age[0], age[1], age[2], age[3]);
        }

        readProtocol(fdRead, message, bufferSize);
//        printf("\t\t\tD or F or C or E %s\n", message);
      }
    }
  }

}
