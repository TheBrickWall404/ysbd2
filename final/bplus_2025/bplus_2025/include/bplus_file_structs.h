//
// Created by theofilos on 11/4/25.
//

#ifndef BP_FILE_STRUCTS_H
#define BP_FILE_STRUCTS_H
#include "bf.h"
#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "record.h"
//#include "bplus_file_structs.h" -> Αυτό δεν είναι λάθος;


typedef enum {      //ο ¨τυπος" του μπλοκ
    BP_TYPE_INDEX,
    BP_TYPE_DATA
} BlockType;

typedef struct {        //metadata του μπλοκ 0
    int root_block_id;      // Η ρίζα του μπλοκ μας ειναι το id 
    int file_type_magic;    // Magic number για αναγνώρηση τύπου αρχείου (αν ειναι όντως αρχείο b+ tree)
    TableSchema schema;     // Το σχήμα του πίνακα
} BPlusMeta;


typedef struct {        // index or data
    BlockType type;         
    int count;              // πλήθος στοιχειων μεσα στο μπλοκ για να ξερουμε που τελειωνει 
    int next_block_id;      // Αν ειναι data -> next , αν ειναι index δειχνει στο αριστεροτερο μπλοκ child
} BlockHeader;


typedef struct {        //εγγραφη index
    int key;
    int block_id;       //ποιντερ προς τα δεξιά
} IndexEntry;

#endif