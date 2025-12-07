#include <string.h>
#include "bplus_index_node.h"
#include "bf.h"

void indexnode_init(char* data_ptr) {
    BlockHeader* header = (BlockHeader*)data_ptr;
    header->type = BP_TYPE_INDEX;
    header->count = 0;
    header->next_block_id = -1; // Θα χρησιμοποιηθεί ως P0 (Leftmost pointer)
}

int indexnode_insert(char* data_ptr, int key, int right_child_block_id) {
    BlockHeader* header = (BlockHeader*)data_ptr;
    int entry_size = sizeof(IndexEntry);
    int max_entries = (BF_BLOCK_SIZE - sizeof(BlockHeader)) / entry_size;

    if (header->count >= max_entries) {
        return -1; // Full -> Trigger Index Split
    }

    IndexEntry* entries = (IndexEntry*)(data_ptr + sizeof(BlockHeader));
    
    int i;
    for (i = 0; i < header->count; i++) {
        if (entries[i].key > key) break;
    }

    // Shift δεξιά
    if (i < header->count) {
        memmove(&entries[i + 1], &entries[i], (header->count - i) * entry_size);
    }

    // Insert
    entries[i].key = key;
    entries[i].block_id = right_child_block_id;
    header->count++;

    return 0;
}