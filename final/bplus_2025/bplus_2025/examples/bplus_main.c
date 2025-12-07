#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "bplus_file_funcs.h"
#include "record_generator.h"

#define RECORDS_NUM 200 // Number of random records to insert & search

// Macro to handle BF library errors
#define CALL_OR_DIE(call)     \
{                             \
  BF_ErrorCode code = call;   \
  if (code != BF_OK) {        \
    BF_PrintError(code);      \
    exit(code);               \
  }                           \
}

// Forward declarations
void insert_records(TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name);

void search_records(TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name);

int main() {
  // ===== Employee test =====
  const TableSchema employee_schema = employee_get_schema();
  insert_records(employee_schema, employee_random_record, "employees.db");
  search_records(employee_schema, employee_random_record, "employees.db");

  // ===== Student test =====
  const TableSchema student_schema = student_get_schema();
  insert_records(student_schema, student_random_record, "students.db");
  search_records(student_schema, student_random_record, "students.db");

  return 0;
}

/**
 * Inserts random records into a B+ tree file.
 */
void insert_records(const TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name)
{
  BF_Init(LRU);

  // Create and open B+ tree file
  bplus_create_file(&schema, file_name);

  int file_desc;
  BPlusMeta* info;
  bplus_open_file(file_name, &file_desc, &info);

  Record record;
  srand(42); // Deterministic random sequence for reproducibility

  // Insert random records
  for (int i = 0; i < RECORDS_NUM; i++) {
    random_record(&schema, &record);

    printf("Insert value: %d\n", record_get_key(&schema, &record));
    record_print(&schema, &record);

    bplus_record_insert(file_desc, info, &record);
  }

  // Clean up
  bplus_close_file(file_desc, info);
  BF_Close();
}

/**
 * Searches for random records in the B+ tree file (should find most of them).
 */
void search_records(const TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name)
{
  srand(42); // Same seed so the same random keys are searched

  BF_Init(LRU);

  int file_desc;
  BPlusMeta* info;
  bplus_open_file(file_name, &file_desc, &info);

  // Searching for the keys 151012 and 16448
  Record* result = malloc(sizeof(Record));
  int keys[] = {151012, 16448};
  for (int i = 0; i < 2; i++) {
    bplus_record_find(file_desc, info, keys[i], &result);
    if (result != NULL) {
      record_print(&schema, result);
    } else {
      printf("No such record\n");
    }
  }
  free(result);

  bplus_close_file(file_desc, info);
  BF_Close();
}