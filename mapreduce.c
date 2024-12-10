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
#include <stdlib.h>
#include <string.h>

// TODO: add your data structures and related functions here ...

//noeuds pour liste chainée trié
typedef struct Node{
    char *key;
    char *value;
    struct Node *next;
} Node; 

//liste chainée triée
typedef struct 
{
    Node *head;
    pthread_mutex_t lock;
} SortedLinkedList;


int num_partitions;
SortedLinkedList *partitions;
Partitioner partitioner;
Node *currents_node;


void sorted_list_init(SortedLinkedList *list){
    list->head = NULL;
    pthread_mutex_init(&list->lock, NULL);
}

//Fonction d'intertion pour notre liste chainée triée
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
        while (current->next && strcmp(key, current->next->key) > 0)
        {
            current = current->next;
        }
        new_node->next = current->next;
        current->next = new_node;
    }
    pthread_mutex_unlock(&list->lock); 
}

//iterateur 
char* get_next(char* key, int partition_number)
{
    SortedLinkedList* partition = &partitions[partition_number];
    Node*  current = &currents_node[partition_number];
    if (current == NULL)
    {
        Node *node = partition->head;
        while(strcmp(node->key, key) != NULL)
        {
            node = node->next;
        }
        current = node;
        currents_node[partition_number] = *current;
    }
    else
    {
        Node * next = current->next;
        if(strcmp(next->key, key) == NULL){
            return next->key;
        }else
        {
            return NULL;
        }
    }
    
}
// External functions: these are what you must define
void MR_Emit(char *key, char *value) {
    int n = partitioner(key,num_partitions); // on récupère le numéro de la partion avec la fonction de partition déjà choisis
    SortedLinkedList *partition = &partitions[n]; // on récupere la reférence de la liste à la partition n
    sorted_list_insert(partition,key,value);// on insere la nouvelle clef valeur, la méthodde insert s'occupe des mutex
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

    num_partitions = num_reducers; //on a autant de partition que de reducers
    partitions = malloc(sizeof(SortedLinkedList) * num_partitions); //on alloue la taille de partitions, qui correspond a un tableau de list chainée triée
    currents_node = malloc(sizeof(Node) * num_partitions);
    for (int i = 0; i < num_partitions; i++)
        {
            sorted_list_init(&partitions[i]);
        }
    if (partition == NULL) //on verifie que l'utilisatuer a donner une fonction Partioner, si pas on en uitilise une par default
    {
        partitioner = MR_DefaultHashPartition;
    }
    else
    {
        partitioner = partition;
    }

    pthread_t mapper_threads[num_mappers];
    int current_file = 1;
    pthread_mutex_t file_lock; 
    pthread_mutex_init(&file_lock, NULL);

    void * map_work(void *arg) //fontion sur laquel on va envoyer les threads mapper 
    {
        while (1)
        {
            pthread_mutex_lock(&file_lock);
            if (current_file >= argc){
                pthread_mutex_unlock(&file_lock);
                break;
            }
            char *file = argv[current_file++];
            pthread_mutex_unlock(&file_lock);
            map(file);
        }
        return NULL;
    }
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_create(&mapper_threads[i], NULL, map_work, NULL);
    }
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_join(mapper_threads[i], NULL);
    } 
    
    pthread_mutex_destroy(&file_lock);

    pthread_t reducers_threads[num_reducers];
    int current_partition = 0;

    //pas besoin de mutex car chaque thread va travailler sur sa propre partition
    void *reduce_work(void* arg){
        Node current = (int) arg;
        while (current->next != NULL)
        {
            reduce(current->key, get_next, nb_partition);
        }
        return NULL;
    }

    for (int i = 0; i < num_reducers; i++)
    {
        *current_partition = i;
        pthread_create(&reducers_threads[i], NULL, reduce_work, &current_partition);
    }
    for (int i = 0; i < num_reducers; i++)
    {
        pthread_join(reducers_threads[i], NULL);
    }
    


}
