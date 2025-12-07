#include "bplus_file_funcs.h"
#include "bplus_file_structs.h"
#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "bf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Βοηθητικό Macro για errors της BF
#define CALL_BF(call) { BF_ErrorCode c = call; if (c != BF_OK) { BF_PrintError(c); return -1; } }

// --- Υλοποίηση Member A (Create, Open, Close, Find) ---

int bplus_create_file(const TableSchema *schema, const char *fileName)
{
    // 1. Δημιουργία αρχείου στο επίπεδο BF
    CALL_BF(BF_CreateFile(fileName));

    // 2. Άνοιγμα για να γράψουμε το Block 0 (Metadata)
    int fd;
    CALL_BF(BF_OpenFile(fileName, &fd));

    // 3. Allocation του Block 0
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_AllocateBlock(fd, block)); // Γίνεται το Block 0

    // 4. Εγγραφή Metadata
    char* data = BF_Block_GetData(block);
    BPlusMeta* meta = (BPlusMeta*)data;

    meta->file_type_magic = 12345; // Magic number για αναγνώριση
    meta->root_block_id = -1;      // Το δέντρο είναι άδειο αρχικά
    meta->schema = *schema;        // Αντιγραφή του σχήματος

    // 5. Αποθήκευση και κλείσιμο
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
    CALL_BF(BF_CloseFile(fd));

    return 0;
}

int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
    // 1. Άνοιγμα αρχείου
    CALL_BF(BF_OpenFile(fileName, file_desc));

    // 2. Ανάγνωση Block 0
    BF_Block* block;
    BF_Block_Init(&block);
    if (BF_GetBlock(*file_desc, 0, block) != BF_OK) {
        BF_Block_Destroy(&block);
        return -1;
    }

    char* data = BF_Block_GetData(block);
    BPlusMeta* stored_meta = (BPlusMeta*)data;

    // 3. Έλεγχος Magic Number
    if (stored_meta->file_type_magic != 12345) {
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        return -1; // Δεν είναι σωστό αρχείο B+
    }

    // 4. Φόρτωση metadata στη μνήμη (Heap)
    // Ο caller θα πρέπει να ελευθερώσει αυτή τη μνήμη στο Close
    *metadata = malloc(sizeof(BPlusMeta));
    memcpy(*metadata, stored_meta, sizeof(BPlusMeta));

    BF_UnpinBlock(block); // Δεν αλλάξαμε τίποτα, άρα όχι Dirty
    BF_Block_Destroy(&block);

    return 0;
}

int bplus_close_file(int file_desc, BPlusMeta* metadata)
{
    // 1. Ενημέρωση του Block 0 (γιατί μπορεί να άλλαξε το root_block_id)
    BF_Block* block;
    BF_Block_Init(&block);
    
    // Πρέπει να πάρουμε το Block 0
    if (BF_GetBlock(file_desc, 0, block) == BF_OK) {
        char* data = BF_Block_GetData(block);
        // Αντιγράφουμε τα ενημερωμένα metadata από τη μνήμη στο δίσκο
        memcpy(data, metadata, sizeof(BPlusMeta));
        
        BF_Block_SetDirty(block);
        BF_UnpinBlock(block);
    }
    BF_Block_Destroy(&block);

    // 2. Κλείσιμο αρχείου BF
    CALL_BF(BF_CloseFile(file_desc));

    // 3. Αποδέσμευση μνήμης metadata
    free(metadata);

    return 0;
}

int bplus_record_find(int file_desc, const BPlusMeta *metadata, int key, Record** out_record)
{
    *out_record = NULL;

    // Αν το δέντρο είναι άδειο
    if (metadata->root_block_id == -1) {
        return -1; 
    }

    int current_block_id = metadata->root_block_id;
    BF_Block* block;
    BF_Block_Init(&block);

    // Πλοήγηση μέχρι να βρούμε Φύλλο
    while (1) {
        if (BF_GetBlock(file_desc, current_block_id, block) != BF_OK) {
            BF_Block_Destroy(&block);
            return -1;
        }

        char* data = BF_Block_GetData(block);
        BlockHeader* header = (BlockHeader*)data;

        if (header->type == BP_TYPE_INDEX) {
            // --- INDEX NODE ---
            IndexEntry* entries = (IndexEntry*)(data + sizeof(BlockHeader));
            int next_id = header->next_block_id; // Default P0 (Leftmost)

            // Linear search για το σωστό παιδί
            for (int i = 0; i < header->count; i++) {
                if (key < entries[i].key) {
                    break;
                }
                next_id = entries[i].block_id;
            }

            // Αλλαγή μπλοκ
            int prev_id = current_block_id;
            current_block_id = next_id;
            
            BF_UnpinBlock(block); // Ξεκαρφίτσωμα τρέχοντος
            // (Το loop θα ξαναπάρει το νέο block στην αρχή)
        } 
        else {
            // --- DATA NODE (LEAF) ---
            // Είμαστε στο φύλλο, ψάχνουμε το κλειδί
            char* records_start = data + sizeof(BlockHeader);
            int rec_size = metadata->schema.record_size;
            int key_offset = metadata->schema.offsets[metadata->schema.key_index];
            int found = 0;

            for (int i = 0; i < header->count; i++) {
                int current_key = *(int*)(records_start + i * rec_size + key_offset);
                
                if (current_key == key) {
                    // ΒΡΕΘΗΚΕ!
                    *out_record = malloc(sizeof(Record));
                    // Χρήση της deserialize που φτιάξαμε
                    deserialize_record(records_start + i * rec_size, *out_record, &metadata->schema);
                    found = 1;
                    break;
                }
                // Επειδή είναι ταξινομημένα, αν περάσουμε το κλειδί, δεν υπάρχει
                if (current_key > key) break;
            }

            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);

            return found ? 0 : -1;
        }
    }
}

// --- Υλοποίηση Member B (Insert & Helpers) ---

// Δομή για την επιστροφή από την αναδρομή
typedef struct {
    int split;        // 1 αν έγινε split
    int new_block_id; // Το ID του νέου δεξιού block
    int mid_key;      // Το κλειδί που ανεβαίνει στον πατέρα
    int error;        // 1 αν υπήρξε σφάλμα (π.χ. duplicate key, BF error)
} InsertResult;

InsertResult _insert_recursive(int file_desc, int current_block_id, const Record* record, const TableSchema* schema);

int bplus_record_insert(int file_desc, BPlusMeta *metadata, const Record *record)
{
    // 1. Περίπτωση: Άδειο Δέντρο (Δημιουργία πρώτου φύλλου-ρίζας)
    if (metadata->root_block_id == -1) {
        BF_Block* new_block;
        BF_Block_Init(&new_block);
        
        // Error Handling για BF
        if (BF_AllocateBlock(file_desc, new_block) != BF_OK) {
            BF_Block_Destroy(&new_block);
            return -1;
        }
        
        int new_id;
        BF_GetBlockCounter(file_desc, &new_id);
        new_id--; // Το ID του νέου μπλοκ

        datanode_init(BF_Block_GetData(new_block));
        // Δεν χρειάζεται έλεγχος για split εδώ, είναι άδειο
        datanode_insert(BF_Block_GetData(new_block), record, &metadata->schema);

        metadata->root_block_id = new_id;
        
        BF_Block_SetDirty(new_block);
        BF_UnpinBlock(new_block);
        BF_Block_Destroy(&new_block);
        
        return new_id; 
    }

    // 2. Αναδρομική Εισαγωγή
    InsertResult res = _insert_recursive(file_desc, metadata->root_block_id, record, &metadata->schema);

    if (res.error) {
        return -1; // Υπήρξε σφάλμα (π.χ. duplicate key ή BF error)
    }

    // 3. Root Split: Αν η ρίζα έσπασε, φτιάχνουμε νέα ρίζα (Index Node)
    if (res.split) {
        BF_Block* new_root;
        BF_Block_Init(&new_root);
        if (BF_AllocateBlock(file_desc, new_root) != BF_OK) {
            BF_Block_Destroy(&new_root);
            return -1;
        }

        int new_root_id;
        BF_GetBlockCounter(file_desc, &new_root_id); 
        new_root_id--;

        char* data = BF_Block_GetData(new_root);
        indexnode_init(data); // header->next_block_id = -1
        BlockHeader* h = (BlockHeader*)data;

        // Ο παλιός root γίνεται το αριστερό παιδί (P0)
        h->next_block_id = metadata->root_block_id;
        
        // Το mid_key και ο δείκτης στο δεξί τμήμα μπαίνουν ως entry (Key, P1)
        // Σημείωση: Στα Index nodes το κλειδί "ανεβαίνει"
        indexnode_insert(data, res.mid_key, res.new_block_id);

        // Ενημέρωση Metadata
        metadata->root_block_id = new_root_id;

        BF_Block_SetDirty(new_root);
        BF_UnpinBlock(new_root);
        BF_Block_Destroy(&new_root);
    }

    return 0; // Success
}

InsertResult _insert_recursive(int file_desc, int current_block_id, const Record* record, const TableSchema* schema) {
    InsertResult result = {0, -1, 0, 0};
    BF_Block* block;
    BF_Block_Init(&block);
    
    // Safety check: Αν αποτύχει το GetBlock
    if(BF_GetBlock(file_desc, current_block_id, block) != BF_OK) {
        BF_Block_Destroy(&block);
        result.error = 1;
        return result;
    }

    char* data = BF_Block_GetData(block);
    BlockHeader* header = (BlockHeader*)data;

    if (header->type == BP_TYPE_DATA) {
        // --- ΦΥΛΛΟ (Leaf Node) ---
        
        int res = datanode_insert(data, record, schema);
        
        if (res == 0) {
            // Επιτυχία, χωρούσε
            BF_Block_SetDirty(block);
        } 
        else if (res == -2) {
            // Duplicate Key: Σταματάμε!
            printf("Error: Duplicate key %d\n", record_get_key(schema, record));
            result.error = 1; 
            // Δεν κάνουμε SetDirty γιατί δεν αλλάξαμε τίποτα
        }
        else {
            // res == -1: Το φύλλο είναι γεμάτο -> LEAF SPLIT
            BF_Block* new_block;
            BF_Block_Init(&new_block);
            if (BF_AllocateBlock(file_desc, new_block) != BF_OK) {
                result.error = 1;
                BF_UnpinBlock(block);
                BF_Block_Destroy(&block);
                BF_Block_Destroy(&new_block);
                return result;
            }
            
            int new_block_id;
            BF_GetBlockCounter(file_desc, &new_block_id); 
            new_block_id--;
            
            char* new_data = BF_Block_GetData(new_block);
            datanode_init(new_data);

            // Temp Buffer για ταξινόμηση και split
            int rec_size = schema->record_size;
            int total_recs = header->count + 1;
            int split_pt = (total_recs + 1) / 2; // (N+1)/2 records στο αριστερό
            
            char* temp = malloc(total_recs * rec_size);
            char* old_recs = data + sizeof(BlockHeader);
            
            // Logic: Merge old records + new record into temp (Insertion Sort logic)
            int inserted = 0;
            int key = record_get_key(schema, record);
            int key_offset = schema->offsets[schema->key_index];

            for(int i=0, j=0; i < total_recs; i++) {
                int curr_key_old = (j < header->count) ? *(int*)(old_recs + j*rec_size + key_offset) : 2147483647; // MAX_INT
                
                if (!inserted && key < curr_key_old) {
                    serialize_record(temp + i*rec_size, record, schema);
                    inserted = 1;
                } else {
                    memcpy(temp + i*rec_size, old_recs + j*rec_size, rec_size);
                    j++;
                }
            }

            // Ενημέρωση Αριστερού (Τρέχοντος) Κόμβου
            header->count = split_pt;
            memcpy(old_recs, temp, split_pt * rec_size);

            // Ενημέρωση Δεξιού (Νέου) Κόμβου
            BlockHeader* new_h = (BlockHeader*)new_data;
            new_h->count = total_recs - split_pt;
            memcpy(new_data + sizeof(BlockHeader), temp + split_pt * rec_size, new_h->count * rec_size);

            // Update Pointers: Λίστα συνδεδεμένων φύλλων
            new_h->next_block_id = header->next_block_id;
            header->next_block_id = new_block_id;

            // Επιστροφή αποτελέσματος split
            result.split = 1;
            result.new_block_id = new_block_id;
            // Στα Leaf Splits, το κλειδί του split "αντιγράφεται" προς τα πάνω
            // Είναι το πρώτο κλειδί του δεξιού κόμβου
            result.mid_key = *(int*)(temp + split_pt * rec_size + key_offset);

            free(temp);
            BF_Block_SetDirty(new_block);
            BF_UnpinBlock(new_block);
            BF_Block_Destroy(&new_block);
            
            BF_Block_SetDirty(block);
        }
    } 
    else {
        // --- INDEX NODE ---
        IndexEntry* entries = (IndexEntry*)(data + sizeof(BlockHeader));
        int child_to_go = header->next_block_id; // Default: P0 (Leftmost)

        // Find correct child
        int i;
        for(i=0; i < header->count; i++) {
            if (record_get_key(schema, record) < entries[i].key) break;
            child_to_go = entries[i].block_id;
        }

        // --- ΑΝΑΔΡΟΜΗ ---
        result = _insert_recursive(file_desc, child_to_go, record, schema);

        if (result.error) {
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
            return result;
        }

        if (result.split) {
            // Το παιδί έσπασε. Πρέπει να εισάγουμε (mid_key, new_block_id) στον τρέχοντα Index Node
            if (indexnode_insert(data, result.mid_key, result.new_block_id) == 0) {
                BF_Block_SetDirty(block);
                result.split = 0; // Το split σταμάτησε εδώ, χωρούσε
            } else {
                // --- INDEX SPLIT ---
                BF_Block* new_idx;
                BF_Block_Init(&new_idx);
                if (BF_AllocateBlock(file_desc, new_idx) != BF_OK) {
                   result.error = 1;
                   // No cleanup needed here as new_idx failed
                } else {
                    int new_idx_id;
                    BF_GetBlockCounter(file_desc, &new_idx_id); 
                    new_idx_id--;
                    
                    char* new_data = BF_Block_GetData(new_idx);
                    indexnode_init(new_data);

                    // Temp Buffer για Index Entries
                    int entry_sz = sizeof(IndexEntry);
                    int total = header->count + 1;
                    IndexEntry* temp_entries = malloc(total * entry_sz);
                    
                    // Merge sort logic για το temp buffer με το νέο entry που ανεβαίνει
                    int inserted = 0;
                    for(int k=0, m=0; k < total; k++) {
                        if (!inserted && (m >= header->count || result.mid_key < entries[m].key)) {
                            temp_entries[k].key = result.mid_key;
                            temp_entries[k].block_id = result.new_block_id;
                            inserted = 1;
                        } else {
                            temp_entries[k] = entries[m++];
                        }
                    }

                    // Στα Index Splits, το μεσαίο κλειδί PUSH UP (δεν μένει στους κόμβους)
                    int split_pt = total / 2;
                    int key_up = temp_entries[split_pt].key; 

                    // Αριστερός κόμβος (τρέχων)
                    header->count = split_pt;
                    memcpy(entries, temp_entries, split_pt * entry_sz);

                    // Δεξιός κόμβος (νέος)
                    BlockHeader* new_h = (BlockHeader*)new_data;
                    // O pointer που συνόδευε το κλειδί που ανέβηκε γίνεται το P0 του νέου κόμβου
                    new_h->next_block_id = temp_entries[split_pt].block_id;
                    
                    new_h->count = total - split_pt - 1; // -1 γιατί το key_up φεύγει
                    memcpy(new_data + sizeof(BlockHeader), &temp_entries[split_pt + 1], new_h->count * entry_sz);

                    // Update result για τον πατέρα
                    result.split = 1;
                    result.new_block_id = new_idx_id;
                    result.mid_key = key_up;

                    free(temp_entries);
                    BF_Block_SetDirty(new_idx);
                    BF_UnpinBlock(new_idx);
                    BF_Block_Destroy(&new_idx);
                    
                    BF_Block_SetDirty(block);
                }
            }
        }
    }

    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
    return result;
}
