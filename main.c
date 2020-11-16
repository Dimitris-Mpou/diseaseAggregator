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
int pid = -1;

int main(int argc, char const *argv[]) {
  int numWorkers, bufferSize;  // Command Line numbers
  int i, j, z, *workerWrite, *workerRead, commandID, success, fail;
  pid_t *idChilds;
  char inputFile[254], command[256], message[256], name[15];
  char StrNumWorkers[6], StrBufferSize[8], StrId[6];      // Gia execlp
  DIR *data;
  struct dirent *direct;
  Record recData;
  FILE *fd;

    // Diabazw ta command line arguments
  if (argc!=7) {
    printf("Wrong number of command line arguments, programm exiting\n");
    return 2;
  } else {
    for (i = 1; i < argc; i+=2) {
      if (argv[i][0]=='-' && argv[i][1]=='i') {
        strcpy(inputFile, argv[i+1]);
      } else if (argv[i][0]=='-' && argv[i][1]=='w') {
        numWorkers = atoi(argv[i+1]);
        if(numWorkers < 1){
          printf("numWorkers should be at least 1!\n");
          return 1;
        }
      } else if (argv[i][0]=='-' && argv[i][1]=='b') {
        bufferSize = atoi(argv[i+1]);
        if(bufferSize < 3){
          printf("bufferSize should be at least 3!\n");
          return 1;
        }
      } else {
        printf("Wrong command line argument, programm exiting\n");
        return 2;
      }
    }
  }

  sprintf(StrNumWorkers, "%d", numWorkers);
  sprintf(StrBufferSize, "%d", bufferSize);

    // Signals kai mkfofos
  signal(SIGINT, handlerIntQuit);
  signal(SIGQUIT, handlerIntQuit);
  signal(SIGCHLD, handlerChld);
  for (i=0; i<numWorkers; i++){
    sprintf(name, "workerWrite%d", i);
    mkfifo(name, 0666);
    sprintf(name, "workerRead%d", i);
    mkfifo(name, 0666);
  }
  idChilds = malloc(numWorkers * sizeof(pid_t));
  for(i=0; i<numWorkers; i++){          // Dimiourgia numWorkers workers
    if( !(idChilds[i] = fork()) ){ break; }
  }

  if(i == numWorkers){    /////  Parent
    int countryCount, *countryWorker, responses, *seen, count;
    char **countries;
    fd_set fds;
	  struct timeval timeout;

    success = 0;
    fail = 0;
    seen = malloc(numWorkers*sizeof(int));
    workerWrite = malloc(numWorkers*sizeof(int));
    workerRead = malloc(numWorkers*sizeof(int));

    if( (data = opendir(inputFile)) == NULL){printf ("Cannot open directory '%s'\n", inputFile);} // Metrima xwrwn
    countryCount = 0;
    while ((direct = readdir(data)) != NULL) {
      if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
        continue;
      countryCount++;
    }

    for (i=0; i<numWorkers; i++){         // Anoigma pipes
      sprintf(name, "workerWrite%d", i);
      workerWrite[i] = open(name,  O_WRONLY);
      if(workerWrite[i]<0){
        perror("Opening write pipe\n");
      }
      sprintf(name, "workerRead%d", i);
      workerRead[i] = open(name,  O_RDONLY);
      if(workerRead[i]<0){
        perror("Opening read pipe\n");
      }
    }

      // Diabasma xwrwn kai antistoixisi stous workers
    countries = malloc(countryCount * sizeof(char *));
    for(i=0; i<countryCount; i++){
      countries[i] = malloc(32 * sizeof(char));
    }
    closedir(data);
    if( (data = opendir(inputFile)) == NULL){printf ("Cannot open directory '%s'\n", inputFile);}

    i = 0;
    while ((direct = readdir(data)) != NULL) {
      if(strcmp(direct->d_name, ".")==0 || strcmp(direct->d_name, "..")==0 )
        continue;
      strcpy(countries[i], direct->d_name);
      i++;
    }
    closedir(data);
    countryWorker = malloc(countryCount * sizeof(int));
    j = 0;
    for(i=0; i<countryCount; i++){
      countryWorker[i] = j;
      if(j==numWorkers-1)
        j = 0;
      else
        j++;
    }
    for(j=0; j<numWorkers; j++){
      for(i=0; i<countryCount; i++){
        if(countryWorker[i]==j){
          writeProtocol(workerWrite[j], countries[i] , bufferSize);           // Stelnei ta onomata twn xwrwn stous workers
        }
      }
    }
         //   Reading summary statistics
    responses = 0;
    for(i=0; i<numWorkers; i++)
      seen[i] = 0;
    while(responses < numWorkers){
      FD_ZERO(&fds);
      for(i=0; i<numWorkers; i++){
        if(!seen[i])
          FD_SET(workerRead[i], &fds);
      }
      timeout.tv_sec = 2;
      timeout.tv_usec = 0;
      i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
      if(i<0)
        printf("Error\n");

      for(i=0; i<numWorkers; i++){
        if( FD_ISSET(workerRead[i], &fds) ){
          responses++;
          seen[i] = 1;
          readStats(workerRead[i], bufferSize);
        }
      }
    }

    //    Erwtimata
    while(1){
      printf("Give a command\n");
      while(1){           // Perimenw eite signal eite na grapsei o xristis sto pliktrologio
        FD_ZERO(&fds);
        FD_SET(0, &fds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
        if(i > 0)
          break;                    // An egrapse o xristis

        if(end){                                         // An irthe SIGINT or SIGQUIT, exiting
          sprintf(message, "log_file.%d", getpid());    // Create logfile
          fd = fopen(message, "w");
          for(i=0; i<countryCount; i++){
            fprintf(fd,"%s\n", countries[i]);
          }
          fprintf(fd, "TOTAL %d\nSUCCESS %d\nFAIL %d\n", success+fail, success, fail);
          fclose(fd);
          pid = -2; // Gia na min diiourgisei alla stis theseis tous
          for(i=0; i<numWorkers; i++){
            kill(idChilds[i], SIGKILL);
          }
          break;
        }
        if(pid > 0){
          for(i=0; i<numWorkers; i++){
    				if(pid == idChilds[i]){
    					if( !(idChilds[i] = fork()) ){
                sprintf(StrId, "%d", i);
    						execlp("./worker", inputFile, StrNumWorkers, StrBufferSize, StrId, "n", NULL);
    					}
              for(j=0; j<countryCount; j++){
                if(countryWorker[j]==i){
                  close(workerWrite[i]);
                  close(workerRead[i]);
                  sprintf(name, "workerWrite%d", i);
                  workerWrite[i] = open(name, O_WRONLY);
                  sprintf(name, "workerRead%d", i);
                  workerRead[i] = open(name, O_RDONLY);
                  writeProtocol(workerWrite[i], countries[j] , bufferSize);
                }
              }
    				}
          }
          pid = -1;
        }
      }
      if(end)
        break;

      fgets(command, 256, stdin);                 // Alliws diabazw tin entoli
      if ((strlen(command) > 0) && (command[strlen(command) - 1] == '\n')){
        command[strlen (command) - 1] = '\0';
      }
      commandID = commandIdentifier(command, &recData);
      if(commandID == 0){                                 //      /listCountries
        for(i=0; i<countryCount; i++){
          printf("%s : %d\n", countries[i], idChilds[countryWorker[i]]);
        }
        success++;
      }else if(commandID == 1){                           //      /diseaseFrequency xwris country
        sprintf(message, "1 %s %s %s", recData.diseaseID, recData.entryDate, recData.exitDate);
        for(i=0; i<numWorkers; i++){
          writeProtocol(workerWrite[i], message, bufferSize);   //  Prowthei tin erwtisi stous workers
        }
        count = 0;
        responses = 0;
        for(i=0; i<numWorkers; i++)
          seen[i] = 0;
        while(responses < numWorkers){
          FD_ZERO(&fds);
          for(i=0; i<numWorkers; i++){
            if(!seen[i])
              FD_SET(workerRead[i], &fds);
          }
          timeout.tv_sec = 2;
          timeout.tv_usec = 0;
          i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
          if(i<0)
            printf("Error\n");

          for(i=0; i<numWorkers; i++){
            if( FD_ISSET(workerRead[i], &fds) ){
              responses++;
              seen[i] = 1;
              readProtocol(workerRead[i], message, bufferSize);
              count += atoi(message);
            }
          }
        }
        printf("%d\n",count);
        success++;
      }else if(commandID == 2){                           //      /diseaseFrequency me country
        count = 0;
        for(i=0; i<countryCount; i++){
          if(strcmp(countries[i], recData.country)==0){
            sprintf(message, "2 %s %s %s %s", recData.diseaseID, recData.entryDate, recData.exitDate, recData.country);
            writeProtocol(workerWrite[countryWorker[i]], message, bufferSize);   //  Prowthei tin erwtisi ston worker
            break;
          }
        }
        if(i==countryCount){
          fail++;
        }else{
          readProtocol(workerRead[countryWorker[i]], message, bufferSize);
          count = atoi(message);
          printf("%d\n",count);
          success++;
        }
      }else if(commandID == 3){                     // topk-ageRanges
        for(i=0; i<countryCount; i++){
          if(strcmp(countries[i], recData.country)==0){
            sprintf(message, "3 %s %s %s %s %s", recData.recordID, recData.country, recData.diseaseID, recData.entryDate, recData.exitDate);
            writeProtocol(workerWrite[countryWorker[i]], message, bufferSize);   //  Prowthei tin erwtisi ston worker
            break;
          }
        }
        if(i==countryCount){
          printf("Error\n");
          fail++;
          break;
        }
        readProtocol(workerRead[countryWorker[i]], message, bufferSize);
        printf("%s", message);
        success++;
      }else if(commandID == 4){                           //    /searchPatientRecord
        sprintf(message, "4 %s", recData.recordID);
        for(i=0; i<numWorkers; i++){
          writeProtocol(workerWrite[i], message, bufferSize);   //  Prowthei tin erwtisi stous workers
        }
        count = 0;
        responses = 0;
        for(i=0; i<numWorkers; i++)
          seen[i] = 0;
        while(responses < numWorkers){
          FD_ZERO(&fds);
          for(i=0; i<numWorkers; i++){
            if(!seen[i])
              FD_SET(workerRead[i], &fds);
          }
          timeout.tv_sec = 2;
          timeout.tv_usec = 0;
          i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
          if(i<0)
            printf("Error\n");

          for(i=0; i<numWorkers; i++){
            if( FD_ISSET(workerRead[i], &fds) ){
              responses++;
              seen[i] = 1;
              readProtocol(workerRead[i], message, bufferSize);
              if(strcmp(message, "Not Found") != 0)
                printf("%s\n", message);
            }
          }
        }
        success++;
      }else if(commandID == 5 || commandID==7){           //      /numPatientAdmissions  kai numPatientDischarges xwris country
        for(i=0; i<numWorkers; i++){
          sprintf(message, "%d %s %s %s", commandID, recData.diseaseID, recData.entryDate, recData.exitDate);
          writeProtocol(workerWrite[i], message, bufferSize);   //  Prowthei tin erwtisi ston worker
        }
        count = 0;
        responses = 0;
        for(i=0; i<numWorkers; i++)
          seen[i] = 0;
        while(responses < numWorkers){
          FD_ZERO(&fds);
          for(i=0; i<numWorkers; i++){
            if(!seen[i])
              FD_SET(workerRead[i], &fds);
          }
          timeout.tv_sec = 2;
          timeout.tv_usec = 0;
          i = select(FD_SETSIZE, &fds, NULL, NULL, &timeout);
          if(i<0)
            printf("Error\n");

          for(i=0; i<numWorkers; i++){
            if( FD_ISSET(workerRead[i], &fds) ){
              responses++;
              seen[i] = 1;
              readProtocol(workerRead[i], message, bufferSize);
              j = 0;
              while(message[j]!='^'){
                if(message[j]=='_')
                  printf("\n");
                else
                  printf("%c", message[j]);
                j++;
              }
            }
          }
        }
        success++;
      }else if(commandID == 6 || commandID == 8){         //      /numPatientAdmissions  kai numPatientDischarges me country
        sprintf(message, "%d %s %s %s %s", commandID, recData.diseaseID, recData.entryDate, recData.exitDate, recData.country);
        for(i=0; i<countryCount; i++){
          if(strcmp(recData.country, countries[i])==0){
            writeProtocol(workerWrite[countryWorker[i]], message, bufferSize);   //  Prowthei tin erwtisi ston worker
            break;
          }
        }
        if(i == countryCount){
          printf("Error\n");
          fail++;
        }else{
          readProtocol(workerRead[countryWorker[i]], message, bufferSize);
          printf("%s\n", message);
          success++;
        }
      }else if(commandID == 9){                 //                /exit
/*        strcpy(message, "9");
        for(i=0; i<numWorkers; i++){
          writeProtocol(workerWrite[i], message, bufferSize);   //  Prowthei to exit stous workers
        }     */
        sprintf(message, "log_file.%d", getpid());
        fd = fopen(message, "w");
        for(i=0; i<countryCount; i++){
          fprintf(fd,"%s\n", countries[i]);
        }
        fprintf(fd, "TOTAL %d\nSUCCESS %d\nFAIL %d\n", success+fail, success, fail);
        fclose(fd);
        pid = -2; // Gia na min dimiourgisei alla stis theseis tous
        for(i=0; i<numWorkers; i++){
          kill(idChilds[i], SIGKILL);
        }
        break;
      }else{
        printf("Wrong command\n");
        fail++;
      }
    } // while

    while( wait(&i) >0 );         // Perimenw tous workers


    for(i=0; i<numWorkers; i++){
      close(workerWrite[i]);
      close(workerRead[i]);
      sprintf(name, "workerWrite%d", i);
      unlink(name);
      sprintf(name, "workerRead%d", i);
      unlink(name);
    }

    return 0;

  }else{                  // Workers
    sprintf(StrId, "%d", i);
    execlp("./worker", inputFile, StrNumWorkers, StrBufferSize, StrId, "y", NULL);
  }
}

void handlerIntQuit(int signum){
  if(signum == 2)
    signal(SIGINT, handlerIntQuit);
  else
    signal(SIGQUIT, handlerIntQuit);

  end = 1;
}

void handlerChld(int signum){
	signal(SIGCHLD, handlerChld);

	int i;

	if(pid != -2){
		pid = wait (&i);
	}
}

// ./diseaseAggregator -w 8 -b 50 -i /home/..../InputData
