#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "functions.h"

void queueInsert(heapQueue *queue, Heap *heap){
  heapQueue *cur;

  cur = queue;
  if(cur->heap == NULL){  // prwti eisagwgi
    cur->heap = heap;
    return ;
  }
  while(cur->next != NULL){
    cur = cur->next;
  }
  cur->next = malloc(sizeof(heapQueue));
  cur->next->next = NULL;
  cur->next->heap = heap;
}

Heap * queuePop(heapQueue **queue){
  Heap *value;
  heapQueue *cur;

  if(*queue != NULL){
    value = (*queue)->heap;
    cur = *queue;
    *queue = (*queue)->next;
    free(cur);
    return value;
  }else{
    return NULL;
  }
}

void queueEmpty(heapQueue *queue){
  heapQueue *cur, *next;
  cur = queue;
  while(cur != NULL){
    next = cur->next;
    free(cur);
    cur = next;
  }
}

void heapSwap(Heap *a, Heap *b){
  int tempCount, tempAge;

  tempCount = a->count;
  tempAge = a->ageRange;
  a->count = b->count;
  a->ageRange = b->ageRange;
  b->count = tempCount;
  b->ageRange = tempAge;
}

void heapInit(Heap *tree){
  tree->left = NULL;
  tree->right = NULL;
  tree->count = -1;
}

void heapInsert(Heap *tree, int ageRange){
  Heap *cur;
  heapQueue *queue;

  queue = malloc(sizeof(heapQueue));
  queue->next = NULL;
  queue->heap = NULL;

  cur = tree;
  while(1){
    if(cur->count == -1){                  // if empty node insert
      cur->ageRange = ageRange;
      cur->count = 1;
      cur->left = malloc(sizeof(Heap));
      cur->right = malloc(sizeof(Heap));
      heapInit(cur->left);
      heapInit(cur->right);
      queueEmpty(queue);
      break;
    }else if(cur->ageRange == ageRange){  // else if country already exists count++
      cur->count++;
      queueEmpty(queue);
      break;
    }else{                                  // else continue to next node
      queueInsert(queue, cur->left);
      queueInsert(queue, cur->right);
      cur = queuePop(&queue);
    }
  }
}

void heapify(Heap *tree){

  if(tree->count == -1){
    return;
  }

  heapify(tree->left);
  heapify(tree->right);
  if(tree->left != NULL && tree->right != NULL){
    if(tree->count <= tree->left->count && tree->left->count >= tree->right->count){
      heapSwap(tree, tree->left);
      heapify(tree->left);
    }else if(tree->count <= tree->right->count && tree->right->count >= tree->left->count){
      heapSwap(tree, tree->right);
      heapify(tree->right);
    }
  }else if(tree->left != NULL){
    if(tree->count <= tree->left->count){
      heapSwap(tree, tree->left);
    }
  }else if(tree->right != NULL){
    printf("Error\n");
  }
}

int heapPop(Heap *tree, int *ageRange){
  Heap *cur, *prev;
  heapQueue *queue;
  int value;

  if(tree->count == -1){
//    printf("Empty heap\n");
    return -2;
  }

  value = tree->count;
  *ageRange = tree->ageRange;

  queue = malloc(sizeof(heapQueue));
  queue->next = NULL;
  queue->heap = NULL;

  cur = tree;
  while(1){
    if(cur->count == -1){       // if empty node then prev is the last node
      tree->ageRange = prev->ageRange;
      tree->count = prev->count;
      free(prev->left);
      free(prev->right);
      prev->count = -1;
      queueEmpty(queue);
      break;
    }else{                       // else continue to next node
      queueInsert(queue, cur->left);
      queueInsert(queue, cur->right);
      prev = cur;
      cur = queuePop(&queue);
    }
  }
  heapify(tree);
  return value;
}

void heapPrint(Heap *tree){
  if(tree->count != -1){
    printf("Heap ageRange = %d\n", tree->ageRange);
    heapPrint(tree->left);
    heapPrint(tree->right);
  }
}

void heapDelete(Heap *tree){
  if(tree->count != -1){
    heapDelete(tree->left);
    heapDelete(tree->right);
    free(tree);
  }else{
    free(tree);
  }
}
