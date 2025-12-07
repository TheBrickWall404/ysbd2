#ifndef BP_DATANODE_H
#define BP_DATANODE_H

#include "record.h"
#include "bplus_file_structs.h"

// Αρχικοποίηση ενός Data Node (Φύλλου)
void datanode_init(char* data_ptr);

// Εισαγωγή Record σε Data Node. Επιστρέφει 0 αν πέτυχε, -1 αν είναι γεμάτο.
int datanode_insert(char* data_ptr, const Record* record, const TableSchema* schema);

// Βοηθητική για να γράφουμε ένα Record σειριακά στη μνήμη
void serialize_record(char* dest, const Record* record, const TableSchema* schema);

void deserialize_record(const char* src, Record* record, const TableSchema* schema);
#endif