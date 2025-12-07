#ifndef BP_FILE_H
#define BP_FILE_H

#include "record.h"
#include "bplus_file_structs.h"
#include "record_generator.h"
#include "bplus_index_node.h"
#include "bplus_datanode.h"
#include "bf.h"

/**
 * @brief Creates a new empty B+ tree file with the given schema.
 * @param schema Pointer to the TableSchema describing the table.
 * @param fileName Name of the file to create.
 * @return 0 on success, -1 on failure.
 */
int bplus_create_file(const TableSchema *schema, const char *fileName);

/**
 * @brief Opens a B+ tree file and loads its metadata.
 * @param fileName Name of the file to open.
 * @param file_desc Pointer to store the file descriptor.
 * @param metadata Pointer to store the metadata structure (allocated by the function).
 * @return 0 on success, -1 on failure.
 */
int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata);


/**
 * @brief Closes a B+ tree file and frees its metadata.
 * @param file_desc File descriptor of the B+ tree file.
 * @param metadata Pointer to the BPlusMeta structure to free.
 * @return 0 on success, -1 on failure.
 */
int bplus_close_file(int file_desc, BPlusMeta* metadata);


/**
 * @brief Inserts a record into the B+ tree.
 * @param file_desc File descriptor of the B+ tree file.
 * @param metadata Pointer to the BPlusMeta structure of the tree.
 * @param record Record to insert.
 * @return Block ID of inserted record on success, -1 on failure.
 */
int bplus_record_insert(int file_desc, BPlusMeta* metadata, const Record *record);

/**
 * @brief Finds a record in the B+ tree by key.
 * @param file_desc File descriptor of the B+ tree file.
 * @param metadata Pointer to the BPlusMeta structure of the tree.
 * @param key Key value to search for.
 * @param out_record Pointer to store the found record (or NULL if not found).
 * @return 0 if found, -1 if not found.
 */
int bplus_record_find(int file_desc, const BPlusMeta *metadata, int key, Record** out_record);

#endif 
