/***********************************************
 * Project 4: Map-Reduce
 * 
 * Group Number : 10
 * Students     : Matteo Verkeyn Nathan Mahieu
 * 
 * Please explain here your choice of data structures :
 * 
 * 
 * 
 ***********************************************/

#include "mapreduce.h"
#include <pthread.h>

// TODO: add your data structures and related functions here ...
typedef struct Node{
    char *key;
    char *value;
    struct Node *next;
} Node; 

typedef struct 
{
    Node *head;
    pthread_mutex_t lock;
} SortedLinkedList;

void sorted_list_init(SortedLinkedList *list){
    list->head = NULL;
    pthread_mutex_init(&list->lock, NULL);
}

void sorted_list_insert(SortedLinkedList *list, char *key, char *value){
    pthread_mutex_lock(&list->lock);

    Node *new_node = malloc(sizeof(Node));
    new_node->key = strdup(key);
    new_node->value = strdup(value);
    new_node->next = NULL;

    if (!list->head || strcmp(key, list->head->key) < 0)
    {
        new_node->next = list->head;
        list->head = new_node;
    }
    else
    {
        Node *current = list->head;
        while (current->next && strcp(key, current->next->key) > 0)
        {
            current = current->next;
        }
        new_node->next = current->next;
        current->next = new_node;
    }
    pthread_mutext_unlock(&list->lock); 
}

int num_partitions;

SortedLinkedList *partitions;

// External functions: these are what you must define
void MR_Emit(char *key, char *value) {
     
}

// DJB2 Hash function (http://www.cse.yorku.ca/~oz/hash.html)
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = (hash << 5) + hash + c; // = hash * 33 + c
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {

    num_partitions = num_reducers;
    partitions = malloc(sizeof(sorted_list_init) * num_partitions);
    for (int i = 0; i < num_partitions; i++)
        {
            sorted_list_init(&partitions[i]);
        }
            

    // TODO
}