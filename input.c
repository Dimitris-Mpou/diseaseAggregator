#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void countInput(char patientRecordsFile[256], int *inputSize){
	int count;
	char ch;
	FILE *fp;

	fp = fopen(patientRecordsFile,"r");
	count = 0;
	while(1){
		ch = fgetc(fp);
		if(ch == '\n'){
				count++;
		}else if(ch == EOF){
			break;
		}
	}

	*inputSize = count;
	fclose(fp);
}

int compareDates(char Date1[11], char Date2[11]){
	int i;
	char temp1[5], temp2[5];

	if (Date2[0] == '-') {	// if date2 == - return 1
		return 1;
	}
	if(Date1[0] == '-')			// if date2 == - return 1
		return 2;
	// Checking the format
	for(i=0; i<2; i++){
		if (Date1[i]<'0'|| Date1[i]>'9' || Date2[i]<'0' || Date2[i]>'9') {
			printf("Wrong date format programm exiting\n");
			return -1;
		}
	}
	if (Date1[i] != '-' || Date2[i] != '-') {
		printf("Wrong date format programm exiting\n");
		return -1;
	}
	for(i=3; i<5; i++){
		if (Date1[i]<'0'|| Date1[i]>'9' || Date2[i]<'0' || Date2[i]>'9') {
			printf("Wrong date format programm exiting\n");
			return -1;
		}
	}
	if (Date1[i] != '-' || Date2[i] != '-') {
		printf("Wrong date format programm exiting\n");
		return -1;
	}
	for(i=6; i<10; i++){
		if (Date1[i]<'0'|| Date1[i]>'9' || Date2[i]<'0' || Date2[i]>'9') {
			printf("Wrong date format programm exiting\n");
			return -1;
		}
	}

	for(i = 6; i < 10; i++) {
		temp1[i-6] = Date1[i];
		temp2[i-6] = Date2[i];
	}
	temp1[4] = '\0';
	temp2[4] = '\0';
	if(atoi(temp1) < atoi(temp2)){
		return 1;
	}else if(atoi(temp1) > atoi(temp2)){
		return 2;
	}else{
		temp1[0] = Date1[3];
		temp1[1] = Date1[4];
		temp1[2] = '\0';
		temp2[0] = Date2[3];
		temp2[1] = Date2[4];
		temp2[2] = '\0';
		if(atoi(temp1) < atoi(temp2)){
			return 1;
		}else if (atoi(temp1) > atoi(temp2)){
			return 2;
		}else{
			temp1[0] = Date1[0];
			temp1[1] = Date1[1];
			temp1[2] = '\0';
			temp2[0] = Date2[0];
			temp2[1] = Date2[1];
			temp2[2] = '\0';
			if(atoi(temp1) < atoi(temp2)){
				return 1;
			}else if(atoi(temp1) > atoi(temp2)){
				return 2;
			}else{
				return 0;
			}
		}
	}
}

int input(char patientRecordsFile[256], dataHT **data, int size, char date[11]){
	int i, count, flag, bool;
	char ch, temp[3];
	Record temp_record;
	FILE *fp;

	fp = fopen(patientRecordsFile,"r");
	count = 1;

	while(1){
		ch = fgetc(fp); // Diabazw to id
    if(ch == EOF){
			break;
		}
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.recordID[i] = ch;
      i++;
      temp_record.recordID[i] = '\0';
      ch = fgetc(fp);
    }
		if(flag){continue;}

		ch = fgetc(fp); // Diabazw to ENTER / EXIT
		ch = fgetc(fp);
		if(ch=='N')
			bool = 1;
		else
		 bool = 0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      ch = fgetc(fp);
    }
		if(flag){continue;}

    ch = fgetc(fp); // Diabazw to patientFirstName
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.patientFirstName[i] = ch;
      i++;
      temp_record.patientFirstName[i] = '\0';
      ch = fgetc(fp);
			if(ch>='A' && ch <= 'Z'){ch += ('a'-'A');}	// lowerCase
    }
		if(flag){continue;}
    ch = fgetc(fp); // Diabazw to patientLastName
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.patientLastName[i] = ch;
      i++;
      temp_record.patientLastName[i] = '\0';
      ch = fgetc(fp);
			if(ch>='A' && ch <= 'Z'){ch += ('a'-'A');}	// lowerCase
    }
		if(flag){continue;}
    ch = fgetc(fp); // Diabazw to diseaseID
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.diseaseID[i] = ch;
      i++;
      temp_record.diseaseID[i] = '\0';
      ch = fgetc(fp);
			if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    }
		if(flag){continue;}

		ch = fgetc(fp); // Diabazw to age
    i=0;
    while(ch != ' ' && ch!='\n' && ch!=EOF){
      temp[i] = ch;
      i++;
      temp[i] = '\0';
      ch = fgetc(fp);
    }
		temp_record.age = atoi(temp);

		if(ch != '\n' && ch != EOF){
			while(ch != '\n' && ch != EOF){		// Agnow pithana lathos stoixeia (pianei periptwseis eggrafwn me perissotera i ligotera pedia)
				ch = fgetc(fp);
			}
		}else if(ch == '\n'){
//			printf("Calling insert for %s\n", temp_record.recordID);
			if(dataHashtableInsert(data, size, temp_record, bool, date, NULL)){ 			// simainei oti uparxei idi
				return 2;
			}
      count++;
		}else if(ch == EOF){
			if(dataHashtableInsert(data, size, temp_record, bool, date, NULL)){
				return 2;
			}
      count++;
			break;
		}else{		// Unexpected error
      return 1;
		}
	}
	fclose(fp);

  return 0;
}

int commandIdentifier(char command[256], Record *rec){
	int i, j;
	char temp_com[256];

	if(strcmp(command, "/listCountries") == 0){ // /listCountries
		return 0;
	}

	strcpy(temp_com, command);
	temp_com[17] = '\0';
	if(strcmp(temp_com, "/diseaseFrequency") == 0){ // /diseaseFrequency
		i = 18;
		while(command[i] != ' '){
			if(command[i] >= 'a' && command[i] <= 'z'){		// upperCase
				rec->diseaseID[i-18] = command[i] - ('a'-'A');
			}else{
				rec->diseaseID[i-18] = command[i];
			}
			i++;
		}
		rec->diseaseID[i-18] = '\0';
		i++;
		for (j=0; j<10; j++) {
			rec->entryDate[j] = command[i+j];
		}
		rec->entryDate[j] = '\0';
		for (j=0; j<10; j++) {
			rec->exitDate[j] = command[i+j+11];
		}
		rec->exitDate[j] = '\0';
		if(compareDates(rec->entryDate, rec->exitDate) == 2 || compareDates(rec->entryDate, rec->exitDate) == -1){
			return -1;	// ignoring command wrong dates
		}
		i += 21;
		if(command[i] == '\0'){								// xwris xwra
			return 1;
		}else{																// me xwra
			i++;
			rec->country[0] = command[i];
			j = 1;
			while(command[i+j] != '\0'){
				rec->country[j] = command[i+j];
				j++;
			}
			rec->country[j] = '\0';
			return 2;
		}
	}

	strcpy(temp_com, command);
	temp_com[15] = '\0';
	if(strcmp(temp_com, "/topk-AgeRanges") == 0){			 // /topk-AgeRanges
		i = 16;
		while(command[i] != ' '){
			rec->recordID[i-16] = command[i];
			i++;
		}
		rec->recordID[i-16] = '\0';
		i++;
		if(command[i] >= 'a' && command[i] <= 'z'){		// upperCase
			rec->country[0] = command[i] - ('a'-'A');
		}else{
			rec->country[0] = command[i];
		}
		j = 1;
		while(command[i+j] != ' ' && command[i+j] != '\0'){
			if(command[i+j] >= 'A' && command[i+j] <= 'Z'){		// lowerCase
				rec->country[j] = command[i+j] +'a'-'A';
			}else{
				rec->country[j] = command[i+j];
			}
			j++;
		}
		rec->country[j] = '\0';
		i += j + 1;

		j = 0;
		while(command[i+j] != ' ' && command[i+j] != '\0'){
			if(command[i+j] >= 'a' && command[i+j] <= 'z'){		// upperCase
				rec->diseaseID[j] = command[i+j] - ('a'-'A');
			}else{
				rec->diseaseID[j] = command[i+j];
			}
			j++;
		}
		rec->diseaseID[j] = '\0';
		i += j + 1;

		for (j=0; j<10; j++) {
			rec->entryDate[j] = command[i+j];
		}
		rec->entryDate[j] = '\0';
		if(command[i+j] != ' '){
			return 100;
		}
		for (j=0; j<10; j++) {
			rec->exitDate[j] = command[i+j+11];
		}
		rec->exitDate[j] = '\0';
		if(compareDates(rec->entryDate, rec->exitDate) == 2 || compareDates(rec->entryDate, rec->exitDate) == -1){
			return -1;	// ignoring command wrong dates
		}
		return 3;
	}

	strcpy(temp_com, command);
	temp_com[20] = '\0';
	if(strcmp(temp_com, "/searchPatientRecord") == 0){ 			// /searchPatientRecord
		i = 21;
		j = 0;
		while(command[i+j] != ' '){
			rec->recordID[j] = command[i+j];
			j++;
		}
		rec->recordID[j] = '\0';
		i += j+1;
		for(j=0; j<10; j++){
			rec->exitDate[j] = command[i+j];
		}
		rec->exitDate[j] = '\0';
		return 4;
	}

	strcpy(temp_com, command);
	temp_com[21] = '\0';
	if(strcmp(temp_com, "/numPatientAdmissions") == 0){				 //		 /numPatientAdmissions
		i = 22;
		j = 0;
		while(command[i+j] != ' '){
			if(command[i+j] >= 'a' && command[i+j] <= 'z'){		// upperCase
				rec->diseaseID[j] = command[i+j] - ('a'-'A');
			}else{
				rec->diseaseID[j] = command[i+j];
			}
			j++;
		}
		rec->diseaseID[j] = '\0';

		i += j+1;
		for (j=0; j<10; j++) {
			rec->entryDate[j] = command[i+j];
		}
		rec->entryDate[j] = '\0';
		if(command[i+j] != ' '){
			return 100;
		}
		for (j=0; j<10; j++) {
			rec->exitDate[j] = command[i+j+11];
		}
		rec->exitDate[j] = '\0';
		if(compareDates(rec->entryDate, rec->exitDate) == 2 || compareDates(rec->entryDate, rec->exitDate) == -1){
			return -1;	// ignoring command wrong dates
		}
		i += j+11;
		if(command[i] == '\0'){								// xwris xwra
			return 5;
		}else{																// me xwra
			i++;
			rec->country[0] = command[i];
			j = 1;
			while(command[i+j] != '\0'){
				rec->country[j] = command[i+j];
				j++;
			}
			rec->country[j] = '\0';
			return 6;
		}
	}

	strcpy(temp_com, command);
	temp_com[21] = '\0';
	if(strcmp(temp_com, "/numPatientDischarges") == 0){				 //		 /numPatientDischarges
		i = 22;
		j = 0;
		while(command[i+j] != ' '){
			if(command[i+j] >= 'a' && command[i+j] <= 'z'){		// upperCase
				rec->diseaseID[j] = command[i+j] - ('a'-'A');
			}else{
				rec->diseaseID[j] = command[i+j];
			}
			j++;
		}
		rec->diseaseID[j] = '\0';

		i += j+1;
		for (j=0; j<10; j++) {
			rec->entryDate[j] = command[i+j];
		}
		rec->entryDate[j] = '\0';
		if(command[i+j] != ' '){
			return 100;
		}
		for (j=0; j<10; j++) {
			rec->exitDate[j] = command[i+j+11];
		}
		rec->exitDate[j] = '\0';
		if(compareDates(rec->entryDate, rec->exitDate) == 2 || compareDates(rec->entryDate, rec->exitDate) == -1){
			return -1;	// ignoring command wrong dates
		}
		i += j+11;
		if(command[i] == '\0'){								// xwris xwra
			return 7;
		}else{																// me xwra
			i++;
			rec->country[0] = command[i];
			j = 1;
			while(command[i+j] != '\0'){
				rec->country[j] = command[i+j];
				j++;
			}
			rec->country[j] = '\0';
			return 8;
		}
	}

	if(strcmp(command, "/exit") == 0){
		return 9;
	}

	// An i entoli den anagnwristike
	return 1000;
}

int update(char patientRecordsFile[256], dataHT **data, int size, Hashtable **disease, int diseaseHashtableNumOfEntries, int bucketEntryCapacity, char date[11]){
	int i, count, flag, bool;
	char ch, temp[3];
	dataHT *cur;
	Record temp_record, *recAddress;
	FILE *fp;

	fp = fopen(patientRecordsFile,"r");
	count = 1;

	while(1){
		ch = fgetc(fp); // Diabazw to id
    if(ch == EOF){
			break;
		}
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.recordID[i] = ch;
      i++;
      temp_record.recordID[i] = '\0';
      ch = fgetc(fp);
    }
		if(flag){continue;}

		flag = 0;
		ch = fgetc(fp); // Diabazw to ENTER / EXIT
		ch = fgetc(fp);
		if(ch=='N'){	//	ENTER
			bool = 1;
			cur = data[Hashfunction(temp_record.recordID, size)];
			while(cur->next != NULL){
		    if(strcmp(temp_record.recordID, cur->record.recordID) == 0){
					flag = 1;		// An uparxei idi i eggrafi
				}
				cur = cur->next;
			}
		}else{				// 	EXIT
		 bool = 0;
	 }
	 if(flag){
		 ch = fgetc(fp);
		 while(ch!='\n' && ch!=EOF){
			 ch = fgetc(fp);
		 }
		 if(ch==EOF)
		 	return 0;
		 continue;
	 }	// paw stin epomeni

		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      ch = fgetc(fp);
    }
		if(flag){continue;}

    ch = fgetc(fp); // Diabazw to patientFirstName
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.patientFirstName[i] = ch;
      i++;
      temp_record.patientFirstName[i] = '\0';
      ch = fgetc(fp);
			if(ch>='A' && ch <= 'Z'){ch += ('a'-'A');}	// lowerCase
    }
		if(flag){continue;}
    ch = fgetc(fp); // Diabazw to patientLastName
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.patientLastName[i] = ch;
      i++;
      temp_record.patientLastName[i] = '\0';
      ch = fgetc(fp);
			if(ch>='A' && ch <= 'Z'){ch += ('a'-'A');}	// lowerCase
    }
		if(flag){continue;}
    ch = fgetc(fp); // Diabazw to diseaseID
		if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    i=0;
		flag = 0;
    while(ch != ' '){
			if(ch == '\n'){	// An leipei to trexwn i to epomeno pedio
//				printf("An wrong record was ignored!\n");
				flag = 1;
				break;
			}
      temp_record.diseaseID[i] = ch;
      i++;
      temp_record.diseaseID[i] = '\0';
      ch = fgetc(fp);
			if(ch>='a' && ch <= 'z'){ch -= ('a'-'A');}	// upperCase
    }
		if(flag){continue;}

		ch = fgetc(fp); // Diabazw to age
    i=0;
    while(ch != ' ' && ch!='\n' && ch!=EOF){
      temp[i] = ch;
      i++;
      temp[i] = '\0';
      ch = fgetc(fp);
    }
		temp_record.age = atoi(temp);

		if(ch != '\n' && ch != EOF){
			while(ch != '\n' && ch != EOF){		// Agnow pithana lathos stoixeia (pianei periptwseis eggrafwn me perissotera i ligotera pedia)
				ch = fgetc(fp);
			}
		}else if(ch == '\n'){
//			printf("Calling insert for %s\n", temp_record.recordID);
			if(dataHashtableInsert(data, size, temp_record, bool, date, &recAddress)){ 			// simainei oti uparxei idi
				return 2;
			}
			if(bool){
				HashtableInsert(disease, diseaseHashtableNumOfEntries, bucketEntryCapacity, recAddress, recAddress->diseaseID, 'd');
			}
      count++;
		}else if(ch == EOF){
			if(dataHashtableInsert(data, size, temp_record, bool, date, &recAddress)){
				return 2;
			}
			if(bool){
				HashtableInsert(disease, diseaseHashtableNumOfEntries, bucketEntryCapacity, recAddress, recAddress->diseaseID, 'd');
			}
      count++;
			break;
		}else{		// Unexpected error
      return 1;
		}
	}
	fclose(fp);

  return 0;
}
