#ifndef BP_INDEXNODE_H
#define BP_INDEXNODE_H

#include "bplus_file_structs.h"

// Αρχικοποίηση Index Node
void indexnode_init(char* data_ptr);

// Εισαγωγή ζεύγους (Key, BlockID). Επιστρέφει 0 αν πέτυχε, -1 αν είναι γεμάτο.
int indexnode_insert(char* data_ptr, int key, int right_child_block_id);

#endif