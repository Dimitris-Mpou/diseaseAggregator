#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/wait.h>
#include <sys/types.h>
#include  <sys/stat.h>
#include <sys/select.h>
#include  <fcntl.h>
#include <signal.h>
#include "structs.h"
#include "functions.h"

#define bucketEntryCapacity  4
#define diseaseHashtableNumOfEntries 3

int end = 0;
int usr = 0;

int main(int argc, char *argv[]){
  int i, j, z, numWorkers, bufferSize, success, fail, myCountriesCount, fileCount, recCount, inputSize, dataHashtableSize;
  int count, age, ages[4], sequence[4], flag, fdWrite, fdRead, id;
  char **myCountries, **files, path[254], *tempPath, tempDate[11], inputFile[254], message[256], name[15];
  dataHT ***records, *cur;      // Enas HT gia kathe xwra, gi auto to 3o asteraki
  Hashtable ***disease, *curr;
  Record recData;
  Heap *heap;
  DIR *data;
  FILE *fd;
  struct dirent *direct;
  fd_set fds;
  struct timeval timeout;

  if(argc != 5){
    printf("Wrong arguments\n");
    return 0;
  }else{
    strcpy(inputFile, argv[0]);
    numWorkers = atoi(argv[1]);
    bufferSize = atoi(argv[2]);
    id = atoi(argv[3]);
  }

    // Anoigma pipes kai signals
  sprintf(name, "workerWrite%d", id);
  fdRead = open(name,  O_RDONLY);
  if(fdRead<0){
    perror("Opening read pipe worker");
  }
  sprintf(name, "workerRead%d", id);
  fdWrite = open(name,  O_WRONLY);
  if(fdWrite<0){
    perror("Opening write pipe worker");
  }
  signal(SIGINT, handlerIntQuit);
  signal(SIGQUIT, handlerIntQuit);
  signal(SIGUSR1, handlerUsr1);

  success = 0;
  fail = 0;

    // Ypologizei poses xwres tou analogoun kai tis diabazei ap to pipe
  if( (data = opendir(inputFile)) == NULL){printf ("Cannot open directory '%s'\n", inputFile);} // Metrima xwrwn
  i = 0;
  while ((direct = readdir(data)) != NULL) {
    if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
      continue;
    i++;
  }
  closedir(data);
  myCountriesCount = i/numWorkers;
  if(i%numWorkers > id)
    myCountriesCount++;
  myCountries = malloc(myCountriesCount * sizeof(char *));
  for(i=0; i<myCountriesCount; i++){
    myCountries[i] = malloc(32 * sizeof(char));
  }
  for(i=0; i<myCountriesCount; i++){
    readProtocol(fdRead, myCountries[i], bufferSize);
  }

    // Dimiourgia Domwn
  records = malloc(myCountriesCount*sizeof(dataHT **));
  disease = malloc(myCountriesCount*sizeof(Hashtable **));
  for(i=0; i<myCountriesCount; i++){   // Anoigma ta directories twn xwrwn tou
    strcpy(path, inputFile);
    strcat(path, "/");
    strcat(path, myCountries[i]);
    if( (data = opendir(path)) == NULL){printf ("Cannot open directory '%s'\n", path);}
    fileCount = 0;
    while ((direct = readdir(data)) != NULL) {
      if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
        continue;
      fileCount++;
    }
    closedir(data);
    files = malloc(fileCount * sizeof(char *));
    for(j=0; j<fileCount; j++){
      files[j] = malloc(11 * sizeof(char));
    }
    if( (data = opendir(path)) == NULL){printf ("Cannot open directory '%s'\n", path);}
    j = 0;
    while ((direct = readdir(data)) != NULL) {    //  Diabasma twn files
      if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
        continue;
      strcpy(files[j], direct->d_name);
      z = j;
      while(z > 0){                   // Kai ta taxinomei me auxousa seira
        if(compareDates(files[z], files[z-1]) == 1){
          strcpy(tempDate, files[z]);
          strcpy(files[z], files[z-1]);
          strcpy(files[z-1], tempDate);
        }else{
          break;
        }
        z--;
      }
      j++;
    }
    closedir(data);
    inputSize = 0;                   // Metraei to megethos tou kathe file wste na ftiaxei to dataHT
    for(j=0; j<fileCount; j++){
      tempPath = malloc(254*sizeof(char));
      strcpy(tempPath, path);
      strcat(tempPath, "/");
      strcat(tempPath, files[j]);
      countInput(tempPath, &recCount);
      inputSize += recCount;
      free(tempPath);
    }
    if(inputSize >= 30){              // Arxikopoiw to dataHashtableSize me basi to megethos tou input
      dataHashtableSize = inputSize/5;
    }else{
      dataHashtableSize = 5;
    }
    records[i] = malloc(dataHashtableSize*sizeof(dataHT *)); // Kanw malloc ton basiko hashtable pou tha periexei tis eggrafes
    for (j=0; j<dataHashtableSize; j++) {
      records[i][j] = malloc(sizeof(dataHT));
    }
    dataHashtableInit(records[i], dataHashtableSize);

    for(j=0; j<fileCount; j++){
      tempPath = malloc(254*sizeof(char));
      strcpy(tempPath, path);
      strcat(tempPath, "/");
      strcat(tempPath, files[j]);
      input(tempPath, records[i], dataHashtableSize, files[j]);  // Diabazw ola ta arxeia pou antistoixoun s auton ton worker
    }
    disease[i] = malloc(diseaseHashtableNumOfEntries*sizeof(Hashtable *)); // Kanw malloc ton disease hashtable
    for(j=0; j<diseaseHashtableNumOfEntries; j++) {
      disease[i][j] = malloc(sizeof(Hashtable));
    }
    HashtableInit(disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity);
    z = 0;
    for(j=0; j<dataHashtableSize; j++) { // Eisagw to arxeio ston diseaseHT
      cur = records[i][j];
      while(cur->next != NULL){
        HashtableInsert(disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity, &(cur->record), cur->record.diseaseID, 'd');
        z++;
        cur = cur->next;
      }
    }

      // Count summary statistics
    if(argv[4][0]=='y')
      summaryStatistics(disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity, fileCount, fdWrite, bufferSize, files, myCountries[i]);

    for(j=0; j<fileCount; j++){
      free(files[j]);
    }
    free(files);
  }
  strcpy(message, "e");                 //    end
  if(argv[4][0]=='y')
    writeProtocol(fdWrite, message, bufferSize);

    // Apantaei stis erwtiseis tou parent
  while(1){

    while(1){           // Perimenw eite signal eite na grapsei o parent sto pipe
      FD_ZERO(&fds);
      FD_SET(fdRead, &fds);
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;
      i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
      if(i > 0)
        break;

      if(end){        // An irthe SIGINT or SIGQUIT kanei exit
        sprintf(message, "log_file.%d", getpid());
        fd = fopen(message, "w");
        for(i=0; i<myCountriesCount; i++){
          fprintf(fd,"%s\n", myCountries[i]);
        }
        fprintf(fd, "TOTAL %d\nSUCCESS %d\nFAIL %d\n", success+fail, success, fail);
        fclose(fd);
        for(i=0; i<myCountriesCount; i++){
          hashtableDelete(disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity);
          dataHashtableDelete(records[i], dataHashtableSize);
        }
        free(disease);
        free(records);
        for(i=0; i<myCountriesCount; i++){
          free(myCountries[i]);
        }
        free(myCountries);
        close(fdWrite);
        close(fdRead);
        break;
      }
      if(usr){                            // New Records
        for(i=0; i<myCountriesCount; i++){
          strcpy(path, inputFile);
          strcat(path, "/");
          strcat(path, myCountries[i]);
          if( (data = opendir(path)) == NULL){printf ("Cannot open directory '%s'\n", path);}
          fileCount = 0;
          while ((direct = readdir(data)) != NULL) {
            if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
              continue;
            fileCount++;
          }
          closedir(data);
          files = malloc(fileCount * sizeof(char *));
          for(j=0; j<fileCount; j++){
            files[j] = malloc(11 * sizeof(char));
          }
          if( (data = opendir(path)) == NULL){printf ("Cannot open directory '%s'\n", path);}
          j = 0;
          while ((direct = readdir(data)) != NULL) {    //  Diabasma twn files
            if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
              continue;
            strcpy(files[j], direct->d_name);
            z = j;
            while(z > 0){                   // Kai ta taxinomei me auxousa seira
              if(compareDates(files[z], files[z-1]) == 1){
                strcpy(tempDate, files[z]);
                strcpy(files[z], files[z-1]);
                strcpy(files[z-1], tempDate);
              }else{
                break;
              }
              z--;
            }
            j++;
          }
          closedir(data);
          for(j=0; j<fileCount; j++){
            tempPath = malloc(254*sizeof(char));
            strcpy(tempPath, path);
            strcat(tempPath, "/");
            strcat(tempPath, files[j]);
            update(tempPath, records[i], dataHashtableSize, disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity, files[j]);  // Diabazw ola ta arxeia pou antistoixoun s auton ton worker
          }
        }
        usr = 0;
      }
    }
    if(end)
      break;
      // Alliws diabazei tin erwtisei kai anatrexei stis domes tou
    readProtocol(fdRead, message, bufferSize);
    if(message[0] == '1'){             //      /diseaseFrequency xwris country
      diseaseFrequency(disease, recData, diseaseHashtableNumOfEntries, bucketEntryCapacity, myCountriesCount, message);
      writeProtocol(fdWrite, message, bufferSize);
      success++;
    }else if(message[0] == '2'){      //      /diseaseFrequency me country
      diseaseFrequencyCountry(disease, recData, diseaseHashtableNumOfEntries, bucketEntryCapacity, myCountriesCount, message, myCountries);
      writeProtocol(fdWrite, message, bufferSize);
      success++;
    }else if(message[0] == '3'){            //topk-AgeRanges
      topkAgeRanges(disease, recData, diseaseHashtableNumOfEntries, bucketEntryCapacity, myCountriesCount, message, myCountries, fdWrite, bufferSize, &success);
    }else if(message[0] == '4'){            //    /searchPatientRecord
      searchPatientRecord(records, recData, dataHashtableSize, myCountriesCount, message, fdWrite, bufferSize, &success, &fail);
    }else if(message[0] == '5' || message[0] == '7'){             //  /numPatientAdmissions kai numPatientDischarges
      numPatient(disease, recData, diseaseHashtableNumOfEntries, bucketEntryCapacity, myCountriesCount, message, myCountries);
      writeProtocol(fdWrite, message, bufferSize);
      success++;
    }else if(message[0] == '6' || message[0] == '8'){        //  /numPatientAdmissions kai numPatientDischarges me country
      numPatientCountry(disease, recData, diseaseHashtableNumOfEntries, bucketEntryCapacity, myCountriesCount, fdWrite,  bufferSize, &success, message, myCountries);
    }else if(message[0] == '9'){
      sprintf(message, "log_file.%d", getpid());
      fd = fopen(message, "w");
      for(i=0; i<myCountriesCount; i++){
        fprintf(fd,"%s\n", myCountries[i]);
      }
      fprintf(fd, "TOTAL %d\nSUCCESS %d\nFAIL %d\n", success+fail, success, fail);
      fclose(fd);

      for(i=0; i<myCountriesCount; i++){
        hashtableDelete(disease[i], diseaseHashtableNumOfEntries, bucketEntryCapacity);
        dataHashtableDelete(records[i], dataHashtableSize);
      }
      free(disease);
      free(records);
      for(i=0; i<myCountriesCount; i++){
        free(myCountries[i]);
      }
      free(myCountries);
      break;
    }else{
      fail++;
    }
  } // while

  return 0;
}

void handlerIntQuit(int signum){
  if(signum == 2)
    signal(SIGINT, handlerIntQuit);
  else
    signal(SIGQUIT, handlerIntQuit);

  end = 1;
}

void handlerUsr1(int signum){
  signal(SIGUSR1, handlerUsr1);

  usr = 1;
}

// gcc -o worker worker.c input.c hashtables.c avlTree.c heap.c protocol.c countStats.c queries.c
