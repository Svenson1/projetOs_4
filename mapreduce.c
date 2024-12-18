/***********************************************
 * Project 4: Map-Reduce
 * 
 * Group Number : 10
 * Students     : Matteo Verkeyn Nathan Mahieu
 * 
 * Please explain here your choice of data structures :
 * on a choisit une liste chainée triée
 * la liste est triée pendant l'insertion cela permet de considérer les liste tout le temps triée
 * les méthode d'intération sur une liste chainée sont aussi rapide et efficace.asm
 * On s'est rendu compte une fois le travail fini que notre structure de donnée etait pas la meilleur. En effet l'insertion trié
 * fais que notre programme a une complexitée en O(n^2)
 * On a pas eu le temps de refaire le projet mais on a penser a une meilleure solution:
 * On pense que on aurait pu utiliser des tableau a la place de liste chainée, en effet le trie d'un tableau ce fait en O(nlog(n))
 * ce qui est largement mieux
 ***********************************************/

#include "mapreduce.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// TODO: add your data structures and related functions here ...

/**********************
* structures globales *
**********************/

//noeuds pour liste chainée trié
typedef struct Node
{
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


// arguments de la fonction map
typedef struct
{
    int argc;
    char ** argv;
    int * current_file;
    pthread_mutex_t *file_lock;
    Mapper map;
}mapWorkArgs;


// arguments de la fonction reduce
typedef struct
{
    int partition;
    Reducer reduce;
}reduceWorkArgs;


/*********************
* variables globales *
**********************/
int num_partitions; // nombres de partitions
SortedLinkedList *partitions; // tableau de liste (le tableau de partition)
Partitioner partitioner; // reference vers le partionner
Node **currents_node; // tableau de noeud dans lequel on garde la reference vers le noeud actuel de la liste

/************************
* fonctions et methodes *
************************/

// initialisation des liste 
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


// fonction qui free la struc list ainsi que les noeuds 
void free_list(SortedLinkedList list)
{
    Node* current = list.head;
    while(current){
        Node * temp = current;
        current = current->next;
        free(temp->key);
        free(temp->value);
        free(temp);
    }
    pthread_mutex_destroy(&list.lock);
}


//iterateur 
char* get_next(char* key, int partition_number)
{
    Node*  current = currents_node[partition_number];
    if (current == NULL)
    {
        return NULL;
    }
    if (strcmp((current)->key, key) != 0)
    {
        return NULL;
    }
    char * value = current->value;
    currents_node[partition_number] = current->next;
    return value;
}


// External functions: these are what you must define
//fonction qui permet de partionner les mots
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


//pas besoin de mutex car chaque thread va travailler sur sa propre partition
void *reduce_work(void* arg){
    reduceWorkArgs * rargs = (reduceWorkArgs *) arg;
    int nb_partition = rargs->partition;
    Reducer reduce = rargs->reduce;
    Node *node = partitions[nb_partition].head;
    char* key = node->key;
    node = node->next;
    reduce(key,get_next,nb_partition);
    while (node != NULL)
    {
        if(strcmp(node->key,key) != 0)
        {  
            key = node->key;
            reduce(key, get_next, nb_partition);
        }
        node = node->next;
    }
    free(rargs);
    return 0;
}


// fonction qui permet de lancer les thread
void * map_work(void *arg) //fontion sur laquel on va envoyer les threads mapper 
    {
        mapWorkArgs *args = (mapWorkArgs*) arg;
        while (1)
        {   
            pthread_mutex_lock(args->file_lock);
            if (*(args->current_file) >= args->argc){
                pthread_mutex_unlock(args->file_lock);
                break;
            }
            char *file = args->argv[(*(args->current_file))++];
            pthread_mutex_unlock(args->file_lock);
            args->map(file);
        }
        return NULL;
    }


// fonction qui crée et lance les thread ainsi que les variables
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {

    num_partitions = num_reducers; //on a autant de partition que de reducers
    partitions = malloc(sizeof(SortedLinkedList) * num_partitions); //on alloue la taille de partitions, qui correspond a un tableau de list chainée triée
    currents_node = calloc(num_partitions, sizeof(Node *));

    for (int i = 0; i < num_partitions; i++) // initialisation des listes
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
    mapWorkArgs args = {argc,argv, &current_file, &file_lock, map};

    for (int i = 0; i < num_mappers; i++)// creation des thread mapper
    {
        pthread_create(&mapper_threads[i], NULL, map_work, &args);
    }
    for (int i = 0; i < num_mappers; i++) // attende de tout les thread avant de continuer
    {
        pthread_join(mapper_threads[i], NULL);
    } 
    
    pthread_mutex_destroy(&file_lock);

    for(int i = 0; i< num_reducers; i++) // initialisation des noeuds courrants avec la tête de chque liste des partitions
    {
        currents_node[i] = partitions[i].head;
    }

    pthread_t reducers_threads[num_reducers];
    for (int i = 0; i < num_reducers; i++) // création des threads reducer
    {
        if(partitions[i].head==NULL)
        {
            continue;
        }
        reduceWorkArgs *rargs = malloc(sizeof(reduceWorkArgs));
        rargs->partition = i;
        rargs->reduce = reduce; 
        pthread_create(&reducers_threads[i], NULL, reduce_work, rargs);
    }
    for (int i = 0; i < num_reducers; i++) // on attend la fin des threads reducer 
    {
        if(partitions[i].head==NULL)
        {
            continue;
        }
        pthread_join(reducers_threads[i], NULL);
    }
    for (int i = 0; i < num_partitions; i++) // free de toute les liste 
    {
        free_list(partitions[i]);
    }
    
    free(partitions); // free du tableau de partition
    free(currents_node);// free du tableau de noeud courrant
    
}
