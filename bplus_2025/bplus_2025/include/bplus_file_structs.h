#ifndef BP_FILE_STRUCTS_H
#define BP_FILE_STRUCTS_H

#include "record.h"

/* Τύπος μπλοκ */
typedef enum {
    BP_TYPE_INDEX,
    BP_TYPE_DATA
} BlockType;

/* Μεταδεδομένα αρχείου (Block 0) */
typedef struct {
    int root_block_id;      // Το ID του μπλοκ που είναι η ρίζα
    int file_type_magic;    // Ένας αριθμός ελέγχου (π.χ. 12345)
    TableSchema schema;     // Το σχήμα του πίνακα
} BPlusMeta;

/* Κεφαλίδα για κάθε κόμβο (Index ή Data) */
typedef struct {
    BlockType type;         
    int count;              // Πόσα στοιχεία έχει μέσα
    int next_block_id;      // Για Data: επόμενο φύλλο. Για Index: δείκτης στο αριστερότερο παιδί (P0)
} BlockHeader;

/* Δομή για εγγραφή στο Index (Key + Pointer προς τα δεξιά) */
typedef struct {
    int key;
    int block_id; 
} IndexEntry;

#endif