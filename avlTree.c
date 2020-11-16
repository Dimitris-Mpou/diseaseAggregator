#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void AVLinit(avlTree *tree){
  tree->left = NULL;
  tree->right = NULL;
  tree->equal = NULL;
  tree->rec = NULL;
  tree->depth = -1;
}

int AVLinsert(avlTree **treeRef, Record *record){
  int result, depth;
  avlTree *tree;
  avlList *cur, *prev;

  tree = *treeRef;

  if(tree->rec != NULL){
    result = compareDates(record->entryDate, tree->rec->entryDate);
    if(result == 1){
      depth = AVLinsert(&(tree->left), record) + 1;
      if(depth > tree->depth){
        tree->depth = depth;
      }
      if(tree->left->depth == tree->right->depth +2){   // An den isxuei i idiotita AVL tote rotation
        AVLrotation(treeRef, record->entryDate);
      }
      return tree->depth;
    }else if(result == 2){
      depth = AVLinsert(&(tree->right), record) + 1;
      if(depth > tree->depth){
        tree->depth = depth;
      }
      if(tree->right->depth == tree->left->depth +2){   // An den isxuei i idiotita AVL tote rotation
        AVLrotation(treeRef, record->entryDate);
      }
      return tree->depth;
    }else if(result == 0){
      if(tree->equal == NULL){
        tree->equal = malloc(sizeof(avlList *));
        tree->equal->rec = record;
        tree->equal->next = NULL;
      }else{
        prev = tree->equal;
        cur = prev->next;
        while(cur != NULL){
          prev = cur;
          cur = cur->next;
        }
        prev->next = malloc(sizeof(avlList *));
        prev->next->rec = record;
        prev->next->next = NULL;
      }
      return tree->depth;
    }else{
      printf("Wrong date!\n");
      return -10;
    }
  }else{
    tree->rec = record;
    tree->depth = 0;
    tree->left = malloc(sizeof(avlTree));
    tree->right = malloc(sizeof(avlTree));
    AVLinit(tree->left);
    AVLinit(tree->right);
    return 0;
  }
}

void AVLrotation(avlTree **treeRef, char recDate[11]){
  int result;
  avlTree *z, *y, *x;

  z = *treeRef;

  result = compareDates(recDate, z->rec->entryDate);
  if(result == 1){
    result = compareDates(recDate, z->left->rec->entryDate);
    if(result == 1){    // left left rotation
      y = z->left;
      x = y->left;
      z->left = y->right;
      y->right = z;
      *treeRef = y;

      if(z->left->depth > z->right->depth){       // Enimerwnw ta depths
        z->depth = z->left->depth +1;
      }else{
        z->depth = z->right->depth +1;
      }
      if(y->left->depth > y->right->depth){
        y->depth = y->left->depth +1;
      }else{
        y->depth = y->right->depth +1;
      }
    }else{                            // left right rotation
      y = z->left;
      x = y->right;
      y->right = x->left;
      z->left = x->right;
      x->left = y;
      x->right = z;
      *treeRef = x;

      if(y->left->depth > y->right->depth){       // Enimerwnw ta depths
        y->depth = y->left->depth +1;
      }else{
        y->depth = y->right->depth +1;
      }
      if(z->left->depth > z->right->depth){
        z->depth = z->left->depth +1;
      }else{
        z->depth = z->right->depth +1;
      }
      if(x->left->depth > x->right->depth){
        x->depth = x->left->depth +1;
      }else{
        x->depth = x->right->depth +1;
      }
    }
  }else{
    result = compareDates(recDate, z->right->rec->entryDate);
    if(result == 1){    // right left rotation
      y = z->right;
      x = y->left;
      y->left = x->right;
      z->right = x->left;
      x->left = z;
      x->right = y;
      *treeRef = x;

      if(y->left->depth > y->right->depth){       // Enimerwnw ta depths
        y->depth = y->left->depth +1;
      }else{
        y->depth = y->right->depth +1;
      }
      if(z->left->depth > z->right->depth){
        z->depth = z->left->depth +1;
      }else{
        z->depth = z->right->depth +1;
      }
      if(x->left->depth > x->right->depth){
        x->depth = x->left->depth +1;
      }else{
        x->depth = x->right->depth +1;
      }
    }else{                            // right right rotation
      y = z->right;
      x = y->right;
      z->right = y->left;
      y->left = z;
      *treeRef = y;

      if(z->left->depth > z->right->depth){       // Enimerwnw ta depths
        z->depth = z->left->depth +1;
      }else{
        z->depth = z->right->depth +1;
      }
      if(y->left->depth > y->right->depth){
        y->depth = y->left->depth +1;
      }else{
        y->depth = y->right->depth +1;
      }
    }
  }
}

void AVLprint(avlTree *tree){
  avlList *cur;

  if(tree->rec != NULL){
//    if(tree->rec->exitDate[0]=='-')
      printf("\n\t\tRecord\t\n\nRecordID:\t%s\nFirst Name:\t%s\nLast Name:\t%s\nDisease:\t%s\nCountry:\t%s\nEntry Date:\t%s\nExit Date:\t%s\n", tree->rec->recordID, tree->rec->patientFirstName, tree->rec->patientLastName, tree->rec->diseaseID, tree->rec->country, tree->rec->entryDate, tree->rec->exitDate);
    cur = tree->equal;
    while(cur != NULL){
//      if(cur->rec->exitDate[0]=='-')
        printf("\n\t\tRecord\t\n\nRecordID:\t%s\nFirst Name:\t%s\nLast Name:\t%s\nDisease:\t%s\nCountry:\t%s\nEntry Date:\t%s\nExit Date:\t%s\n", cur->rec->recordID, cur->rec->patientFirstName, cur->rec->patientLastName, cur->rec->diseaseID, cur->rec->country, cur->rec->entryDate, cur->rec->exitDate);
      cur = cur->next;
    }

    AVLprint(tree->left);
    AVLprint(tree->right);
  }
}

int AVLcount(avlTree *tree){
  int count;
  avlList *cur;

  count = 0;
  if(tree->rec != NULL){
    count++;

    cur = tree->equal;
    while(cur != NULL){
      count++;
      cur = cur->next;
    }

    count += AVLcount(tree->left);
    count += AVLcount(tree->right);
  }

  return count;
}

int AVLcountDates(avlTree *tree, char dateMin[11], char dateMax[11]){
  int count;
  avlList *cur, *next;

  count = 0;
  if(tree->rec != NULL){
    if((compareDates(dateMin, tree->rec->entryDate)==1 || compareDates(dateMin, tree->rec->entryDate)==0) && (compareDates(tree->rec->entryDate, dateMax)==1 || compareDates(tree->rec->entryDate, dateMax)==0)){
      count++;
      cur = tree->equal;
      while(cur != NULL){
        count++;
        cur = cur->next;
      }
    }
    if(compareDates(dateMin, tree->rec->entryDate)==1){   // if dateMin < recDate
      count += AVLcountDates(tree->left, dateMin, dateMax);
    }
    if(compareDates(tree->rec->entryDate, dateMax)==1){   // if recDate < dateMax
      count += AVLcountDates(tree->right, dateMin, dateMax);
    }
  }

  return count;
}

  int AVLcountDatesExit(avlTree *tree, char dateMin[11], char dateMax[11]){
    int count;
    avlList *cur, *next;

    count = 0;
    if(tree->rec != NULL){
      if((compareDates(dateMin, tree->rec->exitDate)==1 || compareDates(dateMin, tree->rec->exitDate)==0) && (compareDates(tree->rec->exitDate, dateMax)==1 || compareDates(tree->rec->exitDate, dateMax)==0)){
        count++;
      }

      cur = tree->equal;
      while(cur != NULL){
        if((compareDates(dateMin, cur->rec->exitDate)==1 || compareDates(dateMin, cur->rec->exitDate)==0) && (compareDates(cur->rec->exitDate, dateMax)==1 || compareDates(cur->rec->exitDate, dateMax)==0)){
          count++;
        }
        cur = cur->next;
      }

      count += AVLcountDatesExit(tree->left, dateMin, dateMax);
      count += AVLcountDatesExit(tree->right, dateMin, dateMax);
    }

  return count;
}

void AVLcountDatesAge(avlTree *tree, char date[11], int *ageCount){
  avlList *cur, *next;

  if(tree->rec != NULL){
    if( compareDates(date, tree->rec->entryDate) == 0 ){
      if(tree->rec->age <= 20)
        ageCount[0]++;
      else if(tree->rec->age <= 40)
        ageCount[1]++;
      else if(tree->rec->age <= 60)
        ageCount[2]++;
      else
        ageCount[3]++;
      cur = tree->equal;
      while(cur != NULL){
        if(cur->rec->age <= 20)
          ageCount[0]++;
        else if(cur->rec->age <= 40)
          ageCount[1]++;
        else if(cur->rec->age <= 60)
          ageCount[2]++;
        else
          ageCount[3]++;
        cur = cur->next;
      }
    }
    if(compareDates(date, tree->rec->entryDate)==1){   // if dateMin < recDate
      AVLcountDatesAge(tree->left, date, ageCount);
    }
    if(compareDates(tree->rec->entryDate, date)==1){   // if recDate < dateMax
      AVLcountDatesAge(tree->right, date, ageCount);
    }
  }
}

void AVLdelete(avlTree *tree){
  avlList *cur, *next;

  if(tree->rec != NULL){
    cur = tree->equal;
    while(cur != NULL){
      next = cur->next;
      free(cur);
      cur = next;
    }
    AVLdelete(tree->left);
    AVLdelete(tree->right);
    free(tree);
  }else{
    free(tree);
  }
}

void AVLinsertHeapDates(avlTree *tree, Heap *heap, char dateMin[11], char dateMax[11]){
  avlList *cur;

  if(tree->rec != NULL){
    if((compareDates(dateMin, tree->rec->entryDate) == 1 || compareDates(dateMin, tree->rec->entryDate) == 0) && (compareDates(tree->rec->entryDate, dateMax)==1 || compareDates(tree->rec->entryDate, dateMax)==0)){
      if(tree->rec->age < 20)
        heapInsert(heap, 0);
      else if(tree->rec->age < 40)
        heapInsert(heap, 1);
      else if(tree->rec->age < 60)
        heapInsert(heap, 2);
      else
        heapInsert(heap, 3);
    }
    cur = tree->equal;
    while(cur!=NULL){
      if(cur->rec->age < 20)
        heapInsert(heap, 0);
      else if(cur->rec->age < 40)
        heapInsert(heap, 1);
      else if(cur->rec->age < 60)
        heapInsert(heap, 2);
      else
        heapInsert(heap, 3);
      cur = cur->next;
    }
    if(compareDates(dateMin, tree->rec->entryDate)==1){   // if dateMin < recDate
      AVLinsertHeapDates(tree->left, heap, dateMin, dateMax);
    }
    if(compareDates(tree->rec->entryDate, dateMax)==1){   // if recDate < dateMax
      AVLinsertHeapDates(tree->right, heap, dateMin, dateMax);
    }
  }
}

int AVLcountPatients(avlTree *tree){
  int count;

  count = 0;
  if(tree->rec != NULL){
    if(tree->rec->exitDate[0] == '-'){
      count++;
    }
    count += AVLcountPatients(tree->left);
    count += AVLcountPatients(tree->right);
  }

  return count;
}
