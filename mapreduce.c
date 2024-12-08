/***********************************************
 * Project 4: Map-Reduce
 * 
 * Group Number : 
 * Students     : Matteo Verkeyn Nathan Mahieu
 * 
 * Please explain here your choice of data structures :
 * 
 * 
 * 
 ***********************************************/

#include "mapreduce.h"

// TODO: add your data structures and related functions here ...

// External functions: these are what you must define
void MR_Emit(char *key, char *value) {
    // TODO
}

// DJB2 Hash function (http://www.cse.yorku.ca/~oz/hash.html)
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = (hash << 5) + hash + c; // = hash * 33 + c
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition) {
    // TODO
}