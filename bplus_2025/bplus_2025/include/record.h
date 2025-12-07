//
// Created by theofilos on 10/30/25.
//

#ifndef BPLUS_MY_RECORD_H
#define BPLUS_MY_RECORD_H
#define MAX_ATTRIBUTES 5
#define MAX_NAME_LENGTH 15
#define MAX_STRING_LENGTH 20

/**
 * @brief Supported attribute data types.
 */
typedef enum {
    TYPE_INT,   /**< Integer type */
    TYPE_CHAR,  /**< Fixed-length string type */
    TYPE_FLOAT, /**< Floating-point type */
    TYPE_NULL   /**< Null/unused type */
} DataType;

/**
 * @brief Schema definition for one attribute (column).
 */
typedef struct {
    char name[MAX_NAME_LENGTH]; /**< Attribute name */
    DataType type;              /**< Data type */
    int length;                 /**< Length (for strings) */
} AttributeSchema;

/**
 * @brief Table schema definition.
 */
typedef struct {
    AttributeSchema attributes[MAX_ATTRIBUTES]; /**< Attribute list */
    int offsets[MAX_ATTRIBUTES];
    int count;                                  /**< Number of attributes */
    int key_index;                              /**< Index of key attribute */
    int record_size;
} TableSchema;

/**
 * @brief Field value storage for one attribute.
 */
typedef union {
    int int_value;                               /**< Integer value */
    float float_value;                           /**< Float value */
    char string_value[MAX_STRING_LENGTH];        /**< String value */
} FieldValue;

/**
 * @brief Record containing values for all schema attributes.
 */
typedef struct {
    FieldValue values[MAX_ATTRIBUTES]; /**< Attribute values */
} Record;


/**
 * @brief Initializes a table schema.
 * @param schema Pointer to the schema to initialize.
 * @param attrs Array of attribute definitions.
 * @param attribute_count Number of attributes in the schema.
 * @param key_attr_name Name of the key attribute.
 */
void schema_init(TableSchema *schema, const AttributeSchema *attrs, int attribute_count, const char *key_attr_name);

/**
 * @brief Prints the table schema.
 * @param schema Pointer to the schema to print.
 */
void schema_print(const TableSchema *schema);



/**
 * @brief Creates a record based on the schema.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record to initialize.
 * @param ... Field values for each attribute in schema order.
 */
void record_create(const TableSchema *schema, Record* record, ...);


/**
 * @brief Prints a record according to the schema.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record to print.
 */
void record_print(const TableSchema *schema, const Record *record);

/**
 * @brief Gets the key value from a record.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record.
 * @return Key value as integer.
 */
int record_get_key(const TableSchema *schema, const Record *record);

/**
 * @brief Retrieves a value from a record by attribute name.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record.
 * @param attr_name Name of the attribute to retrieve.
 * @param output Pointer to store the retrieved value.
 * @return The data type of the returned value.
 */
DataType record_get_value(const TableSchema *schema, const Record *record, const char *attr_name, char *output);




#endif //BPLUS_MY_RECORD_H