#ifndef BP_INDEXNODE_H
#define BP_INDEXNODE_H

#include "bplus_file_structs.h"

void indexnode_init(char* data_ptr);        //αρχικοποιηση index node


int indexnode_insert(char* data_ptr, int key, int right_child_block_id);    //insert key & blockid, 0 αν πετυχε, -1 αν ειναι γεμάτο.

#endif