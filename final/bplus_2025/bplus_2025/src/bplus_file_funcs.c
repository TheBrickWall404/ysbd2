#include "bplus_file_funcs.h"
#include "bplus_file_structs.h"
#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "bf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CALL_BF(call) { BF_ErrorCode c = call; if (c != BF_OK) { BF_PrintError(c); return -1; } }

typedef struct {
    int split;        // 1 -> split
    int new_block_id; // το id του νεου μπλοκ απο τα δεξια
    int mid_key;      // Το κλειδί που ανεβαίνει στον πατέρα
    int error;        // 1 -> error
} InsertResult;

InsertResult _insert_recursive(int file_desc, int current_block_id, const Record* record, const TableSchema* schema);

int bplus_create_file(const TableSchema *schema, const char *fileName)
{
    CALL_BF(BF_CreateFile(fileName)); //create file στον δισκο

    
    int fd;
    CALL_BF(BF_OpenFile(fileName, &fd));    // ανοιγμα αρχειου

    BF_Block* block;        // Κάνουμε allocate το block 0
    BF_Block_Init(&block);
    CALL_BF(BF_AllocateBlock(fd, block)); 

    char* data = BF_Block_GetData(block);   // metadata στο μπλοκ 0.
    BPlusMeta* meta = (BPlusMeta*)data;

    meta->file_type_magic = 12345;  //ορίζουμε ενα magic number (ως 12345) για να αναγνωρίσουμε τον τύπο αρχείου
    meta->root_block_id = -1; //αρχικοποίηση της ρίζασ σε -1 εφόσον δεν υπαρχει ακομα δέντρο.
    meta->schema = *schema; //αντιγραφη του σχηματος του πίνακα

    BF_Block_SetDirty(block);      //setdirty εφόσον το "επεξεργαστήκαμε" και κανουμε την εγγραφή στον δισκο.
    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
    CALL_BF(BF_CloseFile(fd));

    return 0;
}

int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
    CALL_BF(BF_OpenFile(fileName, file_desc));  // ανοιγουμε το αρχειο 
    //και διαβαζουμε το block 0
    BF_Block* block;
    BF_Block_Init(&block);
    if (BF_GetBlock(*file_desc, 0, block) != BF_OK) {
        BF_Block_Destroy(&block);
        return -1;
    }

    char* data = BF_Block_GetData(block);
    BPlusMeta* stored_meta = (BPlusMeta*)data;

    if (stored_meta->file_type_magic != 12345) {        //ελεγχουμε αν ειναι εγκυρο με βαση το magic number μας.
        BF_UnpinBlock(block);   
        BF_Block_Destroy(&block);
        //αν δεν ειναι το σωστο magic number τοτε δεν ειναι δικο μας αρχειο b+ tree.
        return -1;
    }

    *metadata = malloc(sizeof(BPlusMeta));  //αντιγραφη metadata απο μπλοκ στην μνημη για ευκολη προσβαση σε root_id & schema
    memcpy(*metadata, stored_meta, sizeof(BPlusMeta));

    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);

    return 0;
}

int bplus_close_file(int file_desc, BPlusMeta* metadata)    // αν εγιναν αλλαγες στα μεταδεδομενα τις αποθηκευουμε στο μπλοκ 0 του δισκου, κλεινουμε το αρχειο και κανουμε free την μνημη 
{
    BF_Block* block;
    BF_Block_Init(&block);
    
    if (BF_GetBlock(file_desc, 0, block) == BF_OK) {    //Κοιτάμε το μπλοκ 0 για να ενημερώσουμε τα metadata
        char* data = BF_Block_GetData(block);
        memcpy(data, metadata, sizeof(BPlusMeta));  //copy τα updated metadata απο RAM -> BLOCK
        BF_Block_SetDirty(block);   //set dirty για εγγραφή στον δισκο
        BF_UnpinBlock(block);
    }
    BF_Block_Destroy(&block);

    CALL_BF(BF_CloseFile(file_desc));   //κλείνουμε το αρχειο και αποδεσμευουμε την μνήμη που χρησιμοποιήσαμε.
    free(metadata);

    return 0;
}

int bplus_record_find(int file_desc, const BPlusMeta *metadata, int key, Record** out_record)   //Ψαχνουμε με βάση ενα συγκεκριμενο key, ακολουθώντας τους pointers.
{
    *out_record = NULL;
    if (metadata->root_block_id == -1) {    //αν ειναι αδειο το δεντρο -> -1
        return -1; 
    }

    int current_block_id = metadata->root_block_id;
    BF_Block* block;
    BF_Block_Init(&block);

    while (1) {     // tree traversal
        if (BF_GetBlock(file_desc, current_block_id, block) != BF_OK) {
            BF_Block_Destroy(&block);
            return -1;
        }

        char* data = BF_Block_GetData(block);
        BlockHeader* header = (BlockHeader*)data;

        if (header->type == BP_TYPE_INDEX) {        //αν ειναι index ψαχνουμε ποιόν pointer (child) θα ακολουθήσουμε
            IndexEntry* entries = (IndexEntry*)(data + sizeof(BlockHeader));
            int next_id = header->next_block_id; 
            // συγκρινουμε το κλειδί αναζήτησης με τα κλειδια στον κομβο.
            for (int i = 0; i < header->count; i++) {
                if (key < entries[i].key) {
                    break;
                }
                next_id = entries[i].block_id;
            }

            current_block_id = next_id; //παμε στο επόμενο επιπεδο
            BF_UnpinBlock(block); 
        } 
        else {          //σε περίπτωση data node (φυλλο) ψαχνουμε σειριακά το κλειδί μεσα στο μπλοκ
            char* records_start = data + sizeof(BlockHeader);
            int rec_size = metadata->schema.record_size;
            int key_offset = metadata->schema.offsets[metadata->schema.key_index];
            int found = 0;

            for (int i = 0; i < header->count; i++) {
                int current_key = *(int*)(records_start + i * rec_size + key_offset);       //αναγωνση του κλειδιου
                
                if (current_key == key) {       // αν βρέθηκε -> deserialize -> found = 1 -> break
                    *out_record = malloc(sizeof(Record));
                    deserialize_record(records_start + i * rec_size, *out_record, &metadata->schema);
                    found = 1;
                    break;
                }
                if (current_key > key) break;
            }

            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
            return found ? 0 : -1;
        }
    }
}

////

int bplus_record_insert(int file_desc, BPlusMeta *metadata, const Record *record)       //εισαγωγή εγγραφησ, αν το δεντρο ειναι αδειο φτιαχνει την πρωτη ριζα (data node), αν οχι κανει insert recursive. Αν επιστρέψει οτι εγινε split, φτιαχνει νεα ριζα (index node).
{
    if (metadata->root_block_id == -1) {        //αν το δεντρο ειναι αδειο, δεσμευουμε νεο μπλοκ, αρχικοποιουμε ως data node και εισαγουμε εγγραφή.
        BF_Block* new_block;
        BF_Block_Init(&new_block);
        
        if (BF_AllocateBlock(file_desc, new_block) != BF_OK) {
            BF_Block_Destroy(&new_block);
            return -1;
        }
        
        int new_id;
        BF_GetBlockCounter(file_desc, &new_id);
        new_id--;

        datanode_init(BF_Block_GetData(new_block));
        datanode_insert(BF_Block_GetData(new_block), record, &metadata->schema);

        metadata->root_block_id = new_id;       //ενημέρωση root_block_id και μεταδεδομενων.
        
        BF_Block_SetDirty(new_block);
        BF_UnpinBlock(new_block);
        BF_Block_Destroy(&new_block);
        
        return new_id; 
    }

    InsertResult res = _insert_recursive(file_desc, metadata->root_block_id, record, &metadata->schema);    //αναδρομική εισαγωγή

    if (res.error) {
        return -1;
    }

    if (res.split) {        //αν υπαρχει root split :
        BF_Block* new_root;
        BF_Block_Init(&new_root); //δημιουργουμε νεα ριζα (index node)
        if (BF_AllocateBlock(file_desc, new_root) != BF_OK) {
            BF_Block_Destroy(&new_root);
            return -1;
        }

        int new_root_id;
        BF_GetBlockCounter(file_desc, &new_root_id); 
        new_root_id--;

        char* data = BF_Block_GetData(new_root);
        indexnode_init(data); 
        BlockHeader* h = (BlockHeader*)data;

        h->next_block_id = metadata->root_block_id;     //το next_block_id δινει την παλια ριζά
        indexnode_insert(data, res.mid_key, res.new_block_id); //βαζουμε το μεσαιο κλειδι και pointer στο δεξι μπλοκ.

        metadata->root_block_id = new_root_id; //ενημερωνουμε την νεα ριζα

        BF_Block_SetDirty(new_root);
        BF_UnpinBlock(new_root);
        BF_Block_Destroy(&new_root);
    }

    return 0;
}

InsertResult _insert_recursive(int file_desc, int current_block_id, const Record* record, const TableSchema* schema) {      //Κατεβαινουμε αναδρομικα το δεντρο με αυτη την συνάρτηση. Σε data nodes προσπαθουμε να βαλουμε εγγραφη, αν δεν μπροουμε επειδη εχει γεμισει, κανουμε σπλιτ. Σε index βρίσκει το σωστο "παιδι" και καλει τον εαυτό της.

    InsertResult result = {0, -1, 0, 0}; //αρχικοποιηση (ολα "λαθος")
    //διαβάζουμε το block:
    BF_Block* block;
    BF_Block_Init(&block);
    
    if(BF_GetBlock(file_desc, current_block_id, block) != BF_OK) {  //error handling
        BF_Block_Destroy(&block);
        result.error = 1;
        return result;
    }

    char* data = BF_Block_GetData(block);
    BlockHeader* header = (BlockHeader*)data;

    if (header->type == BP_TYPE_DATA) { //αν ειναι Data Node
        int res = datanode_insert(data, record, schema);    //βάζουμε την εγγραφη στο data node
        
        if (res == 0) {
            BF_Block_SetDirty(block);   //χωρεσε, "καταγραφουμε/μαρκαρουμε" την εγγραφη (set dirty)
        } 
        else if (res == -2) {       //duplicate error, υπάρχει το ιδιο κλειδί ηδη.
            printf("Error: Duplicate key %d\n", record_get_key(schema, record));
            result.error = 1; 
        }
        else {      //Το μπλοκ εχει γεμίσει, αρα πρεπει να κάνουμε σπλιτ.
            BF_Block* new_block;
            BF_Block_Init(&new_block); //δεσμευουμε νεο μπλοκ στο δεξι μερος του σπλιτ
            if (BF_AllocateBlock(file_desc, new_block) != BF_OK) {
                result.error = 1;
                BF_UnpinBlock(block);
                BF_Block_Destroy(&block);
                BF_Block_Destroy(&new_block);
                return result;
            }
            
            int new_block_id; // id και δεικτης στα νεα δεδομενα του νεου μπλοκ
            BF_GetBlockCounter(file_desc, &new_block_id); 
            new_block_id--;
            
            char* new_data = BF_Block_GetData(new_block);
            datanode_init(new_data);        //αρχικοποιηση ως αδειο data node
            
            //ταξινόμουμε temp που χωραει ολες τις παλιες εγγραφες μαζι με την καινουργια, ετσι ωστε να τα ταξινομήσουμε ευκολα σωστα.
            int rec_size = schema->record_size;
            int total_recs = header->count + 1; //συνολικες εγγραφες
            
            char* temp = malloc(total_recs * rec_size);//δεσμευση ram 
            char* old_recs = data + sizeof(BlockHeader);//pointer στις παλιες εγγραφες μεσα στο γεματο μπλοκ
            
            int inserted = 0;   //flag για αν εχουμε βαλει νεα εγγραφη στο temp ή οχι.
            int key = record_get_key(schema, record);
            int key_offset = schema->offsets[schema->key_index];

            for(int i=0, j=0; i < total_recs; i++) { //λούπα ταξινόμησης, περναμε μια μια τις θέσεις του temp και βλέπουμε τι θα βάλουμε (παλία ή νεα εγγραφη (j / record))
                int curr_key_old = (j < header->count) ? *(int*)(old_recs + j*rec_size + key_offset) : 2147483647; //κλειδί επομενης παλιας εγγραφης
                
                //Αν δεν εχουμε βαλει την νεα εγγραφη, και το κλειδί της ειναι μικρότερο του παλιού που εχουμε, τοτε βάζουμε νεα εγγραφη στο temp
                if (!inserted && key < curr_key_old) {
                    serialize_record(temp + i*rec_size, record, schema);
                    inserted = 1;
                } else {
                    //αλλιως αντιγραφουμε την παλια και προχωραμε τον pointer j.
                    memcpy(temp + i*rec_size, old_recs + j*rec_size, rec_size);
                    j++;
                }
            }//το temp εχει τωρα ολες τις εγγραφες ταξινομημένες.

            //μοιράζουμε τα δεδομένα μας
            int split_pt = (total_recs + 1) / 2;    //το σημειο του split 
            header->count = split_pt;   // το αριστερό μπλοκ (τρεχον μπλοκ) κρατάει τα μισα (αρχη -> splut_pt)
            memcpy(old_recs, temp, split_pt * rec_size); 
            BlockHeader* new_h = (BlockHeader*)new_data;    //το δεξί, νεο, μπλοκ παιρνει τα υπολοιπα (split_pt -> τελος)
            new_h->count = total_recs - split_pt;
            memcpy(new_data + sizeof(BlockHeader), temp + split_pt * rec_size, new_h->count * rec_size); //αντιγραφή απο την μεση του temp στο νεο block

            //εισαγωγή του νεου μπλοκ στην "αλυσιδα" φυλλων του b+ tree
            new_h->next_block_id = header->next_block_id;   //το νεο μπλοκ δειχνει εκει που εδειχνε το παλιό.
            header->next_block_id = new_block_id;           //το παλιο μπλοκ δείχνει στο νεο μπλοκ (δεξι)

            result.split = 1;  //εγινε σπλιτ
            result.new_block_id = new_block_id; //το id του νεου, δεξιου, block
            result.mid_key = *(int*)(temp + split_pt * rec_size + key_offset);

            free(temp);
            BF_Block_SetDirty(new_block);
            BF_UnpinBlock(new_block);
            BF_Block_Destroy(&new_block);
            
            BF_Block_SetDirty(block);
        }
    } 
    else {
        IndexEntry* entries = (IndexEntry*)(data + sizeof(BlockHeader));  //αν ειναι index
        int child_to_go = header->next_block_id;    //ξεκινάμε με το αριστερο "παιδί"

        int i;
        //"Σκανάρουμε" τα κλειδιά του κομβου
        for(i=0; i < header->count; i++) {  
            if (record_get_key(schema, record) < entries[i].key) break; //αν το κλειδι που αναζητούμε ειναι μικρότερο απο το τρέχον κλειδι, τοτε σταματαμε και κατεβαίνουμε στον pointer που εχει βρει 
            child_to_go = entries[i].block_id; //αλλιως προχωραμε και κρατάμε τον pointer που αντιστοιχει σε αυτο το κλειδί
        }

        result = _insert_recursive(file_desc, child_to_go, record, schema); //αφου βρηκαμε το σωστο child, καλουμε την ιδια συναρτηση για ννα συνεχιστεί η εισαγωγη στο υποδεντρο.

        if (result.error) { //error handling
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
            return result;
        }

        if (result.split) {
            if (indexnode_insert(data, result.mid_key, result.new_block_id) == 0) {
                //το κλειδί χωρεσε!
                BF_Block_SetDirty(block);
                result.split = 0; //το split σταματάει εδω
            } else {
                //Το κλειδί δεν χωρεσε, και ο index node ειναι γεμάτος. Πρεπει να κάνουμε split και το index.
                BF_Block* new_idx;  //φτιαχνουμε ενα νεο index block για να ταξινομήσουμε τα κλειδιά που εχουμε μαζι με το νέο κλειδι
                BF_Block_Init(&new_idx);
                if (BF_AllocateBlock(file_desc, new_idx) != BF_OK) {
                   result.error = 1;
                } else {
                    int new_idx_id; //βρίσκουμε το id του νεου block (συνολο - 1)
                    BF_GetBlockCounter(file_desc, &new_idx_id); 
                    new_idx_id--;
                    
                    char* new_data = BF_Block_GetData(new_idx);  //παιρνουμε τον pointer και τον αρχικοποιουμε ως index.
                    indexnode_init(new_data);
                    
                    //εχουμε Ν παλια keys και 1 καινουργιο, που ηρθε απο το παιδι. Πρεπει να τα βαλουμε σε σειρα να βρουμε το μεσαιο, αλλα δεν χωρανε στο μπλοκ. Αρα φτιαχνουμε temp πινακα στην RAM.

                    int entry_sz = sizeof(IndexEntry);
                    int total = header->count + 1; //τα παλια + το 1 καινουργιο
                    IndexEntry* temp_entries = malloc(total * entry_sz);
                    
                    int inserted = 0;   //Εχει μπεί νεο κλειδί (0=οχι 1=ναι)
                    for(int k=0, m=0; k < total; k++) { //merge loop  |  k που γράφουμε τον temp πινακα  |  m: Ποιό παλιο κλειδί παιρνουμε απο το block
                        if (!inserted && (m >= header->count || result.mid_key < entries[m].key)) {
                            // Βάζουμε το νεο κλειδι που ηρθε απο το child
                            temp_entries[k].key = result.mid_key;
                            temp_entries[k].block_id = result.new_block_id;
                            inserted = 1;
                        } else {
                            //Αλλιώς, αντιγραφουμε το επόμενο παλιό κλειδί
                            temp_entries[k] = entries[m++];
                        }
                    }

                    int split_pt = total / 2; //υπολογίζουμε το μεσαιο
                    int key_up = temp_entries[split_pt].key; //αυτο το κλειδί θα ανεβάσουμε πανω.
                    //αριστερός κομβος
                    header->count = split_pt; //κραταει οσα ειναι πριν το split point
                    memcpy(entries, temp_entries, split_pt * entry_sz);
                    //δεξης κομβος
                    BlockHeader* new_h = (BlockHeader*)new_data;
                    new_h->next_block_id = temp_entries[split_pt].block_id;
                    
                    new_h->count = total - split_pt - 1; //ο δεξιος κομβος θα παρει οσα στοιχεια εμεινα μετα το μεσαιο
                    memcpy(new_data + sizeof(BlockHeader), &temp_entries[split_pt + 1], new_h->count * entry_sz); //αντιγραφουμε τα υπολοιπα κλειδια στον νεο κοβμο

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
