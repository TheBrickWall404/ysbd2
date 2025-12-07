#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "bplus_datanode.h"
#include "bf.h"

void datanode_init(char* data_ptr) {
    BlockHeader* header = (BlockHeader*)data_ptr;
    header->type = BP_TYPE_DATA;
    header->count = 0;
    header->next_block_id = -1; 
}

void serialize_record(char* dest, const Record* record, const TableSchema* schema) {
    for(int i=0; i < schema->count; i++) {
        int offset = schema->offsets[i];
        const AttributeSchema* attr = &schema->attributes[i];
        
        switch(attr->type) {
            case TYPE_INT:
                memcpy(dest + offset, &record->values[i].int_value, sizeof(int));
                break;
            case TYPE_FLOAT:
                memcpy(dest + offset, &record->values[i].float_value, sizeof(float));
                break;
            case TYPE_CHAR:
                memcpy(dest + offset, record->values[i].string_value, attr->length);
                break;
            default: break;
        }
    }
}

int datanode_insert(char* data_ptr, const Record* record, const TableSchema* schema) {
    BlockHeader* header = (BlockHeader*)data_ptr;
    int record_size = schema->record_size;
    
    // Υπολογισμός διαθέσιμου χώρου
    int max_records = (BF_BLOCK_SIZE - sizeof(BlockHeader)) / record_size;

    if (header->count >= max_records) {
        return -1; // Full -> Trigger Split
    }

    int key = record_get_key(schema, record);
    char* records_start = data_ptr + sizeof(BlockHeader);
    int key_offset = schema->offsets[schema->key_index];

    // Εύρεση θέσης (Linear Scan)
    int i;
    for (i = 0; i < header->count; i++) {
        // Διαβάζουμε το κλειδί από τα binary δεδομένα
        int current_key = *(int*)(records_start + i * record_size + key_offset);
        if (current_key > key) break;
        if (current_key == key) return -2; // Duplicate error
    }

    // Shift δεξιά τα υπάρχοντα
    if (i < header->count) {
        memmove(records_start + (i + 1) * record_size,
                records_start + i * record_size,
                (header->count - i) * record_size);
    }

    // Εγγραφή του νέου record
    serialize_record(records_start + i * record_size, record, schema);
    header->count++;
    
    return 0; 
}

void deserialize_record(const char* src, Record* record, const TableSchema* schema) {
    for(int i=0; i < schema->count; i++) {
        int offset = schema->offsets[i];
        const AttributeSchema* attr = &schema->attributes[i];
        
        switch(attr->type) {
            case TYPE_INT:
                memcpy(&record->values[i].int_value, src + offset, sizeof(int));
                break;
            case TYPE_FLOAT:
                memcpy(&record->values[i].float_value, src + offset, sizeof(float));
                break;
            case TYPE_CHAR:
                memcpy(record->values[i].string_value, src + offset, attr->length);
                break;
            default: break;
        }
    }
}