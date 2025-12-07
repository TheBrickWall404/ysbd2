// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων toy Ευρετηρίου.
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "bplus_datanode.h"
#include "bf.h"

void datanode_init(char* data_ptr) {    //αρχικοποίηση
    BlockHeader* header = (BlockHeader*)data_ptr;   //Για να γραψουμε τα metadata στην αρχή του μπλόκ.
    header->type = BP_TYPE_DATA;
    header->count = 0;
    header->next_block_id = -1; 
}

void serialize_record(char* dest, const Record* record, const TableSchema* schema) {    //"Μετατροπή" record, ωστε να μπορει να μπει σε μπλοκ
    for(int i=0; i < schema->count; i++) {
        int offset = schema->offsets[i];
        const AttributeSchema* attr = &schema->attributes[i];
        
        switch(attr->type) {    // αντιγραφή τιμής (int, float  ή string)
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

int datanode_insert(char* data_ptr, const Record* record, const TableSchema* schema) {  //εισαγωγή στο φύλλο του δέντρου, διατηρωντας την ταξινόμηση με βαση το key
    BlockHeader* header = (BlockHeader*)data_ptr;
    int record_size = schema->record_size;
    
    int max_records = (BF_BLOCK_SIZE - sizeof(BlockHeader)) / record_size;  //υπολογίζουμε τον διαθέσιμο χωρο

    if (header->count >= max_records) {
        return -1; // αν δεν εχει χώρο -> -1 (αρα πρεπει να γίνει split)
    }

    int key = record_get_key(schema, record);
    char* records_start = data_ptr + sizeof(BlockHeader);
    int key_offset = schema->offsets[schema->key_index];

    int i;
    for (i = 0; i < header->count; i++) {       // Γραμμική αναζητηση για να βρούμε το key που είναι μεγαλύτερο απο αυτο που θα εισάγουμε
        int current_key = *(int*)(records_start + i * record_size + key_offset);
        if (current_key > key) break;
        if (current_key == key) return -2; // Εαν βρεθεί το ιδιο κλειδί, τοτε -> -2 (οπου δείχνει οτι υπάρχει duplicate)
    }

    // Shift μια θέση προσ τα δεξία για να βάλουμε το νεο record
    if (i < header->count) {
        memmove(records_start + (i + 1) * record_size,
                records_start + i * record_size,
                (header->count - i) * record_size);
    }

    // εγγραφή του νεου record
    serialize_record(records_start + i * record_size, record, schema);
    header->count++;    //αυξηση count
    
    return 0; 
}

void deserialize_record(const char* src, Record* record, const TableSchema* schema) {       //Διαβάζει bytes απο το block και τα φαίρνει σε δομή record ωστε να το χρησιμοποιήθει απο το πρόγραμμα (πχ σε record find etc.)
    for(int i=0; i < schema->count; i++) {  //περνάει ολα τα πεδία του σχηματος 
        int offset = schema->offsets[i];
        const AttributeSchema* attr = &schema->attributes[i];
        
        switch(attr->type) {    //ελέγχουμε τι τύπος δεδομένων ειναι το καθε πεδίο.
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