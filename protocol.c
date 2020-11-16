#include <stdlib.h>
#include <unistd.h>
#include <string.h>

// Temaxizei ta megala minimata se mikrotera megethous buffersize
// Ena mnm teleiwnei me ~
// Paradeigma (buffersize = 8): "Dokimastiko minima" -> "Dokimas" "tiko mi" "nima~" espase to minima se 3 kommatia megethous buffersize (i ligotero)

void writeProtocol(int fd, char *message, int bufferSize){
  int i, j, sent, length;
  char *buffer;

  length = strlen(message);
  if(length < bufferSize-1){
    message[length] = '~';
    message[length+1] = '\0';
    write(fd, message, bufferSize);
    message[length] = '\0';
  }else{
    buffer = malloc(bufferSize * sizeof(char));
    sent = 0;
    while(sent <= length){
      i = sent;
      j = 0;
      while( j < bufferSize-1 && message[i] != '\0'){
        buffer[j] = message[i];
        i++;
        j++;
      }
      if(message[i] == '\0' && j < bufferSize-2){
        buffer[j] = '~';
        buffer[j+1] = '\0';
        j++;
      }else{
        buffer[j] = '\0';
      }

      write(fd, buffer, bufferSize);
      sent+= j;
    }
    free(buffer);
  }
}

void readProtocol(int fd, char *message, int bufferSize){
  int i, j;
  char *buffer;

  buffer = malloc(bufferSize * sizeof(char));
  i = 0;
  while(1){
    read(fd, buffer, bufferSize);
    j = 0;
    while(j < bufferSize && buffer[j] != '~'){
      if(buffer[j] != '\0'){
        message[i] = buffer[j];
        i++;
      }
      j++;
    }
    if(buffer[j] == '~'){ break; }
  }
  message[i] = '\0';
  free(buffer);
}
