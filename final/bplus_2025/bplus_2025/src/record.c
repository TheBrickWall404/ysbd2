#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include "record.h"

void schema_init(TableSchema *schema, const AttributeSchema *attrs, const int attribute_count, const char *key_attr_name) {
    schema->count = attribute_count < MAX_ATTRIBUTES ? attribute_count : MAX_ATTRIBUTES;
    schema->key_index = -1;
    schema->record_size = 0;
    // Calculate offsets and copy attributes
    for (int i = 0; i < schema->count; i++) {
        schema->attributes[i] = attrs[i];
        // Set current offset for this attribute
        schema->offsets[i] = schema->record_size;
        // Calculate size for this attribute and advance record size
        switch (attrs[i].type) {
            case TYPE_INT:
                schema->record_size += sizeof(int);
                break;
            case TYPE_FLOAT:
                schema->record_size += sizeof(float);
                break;
            case TYPE_CHAR:
                schema->record_size += attrs[i].length;
                break;
            default:
                break;
        }
        // Assign primary key if name matches
        if (strcmp(attrs[i].name, key_attr_name) == 0) {
            schema->key_index = i;
        }
    }
    if (schema->key_index == -1) {
        printf("Warning: Key attribute '%s' not found in schema!\n", key_attr_name);
    }
}

void record_create(const TableSchema *schema, Record* record, ...) {
    va_list args;
    va_start(args, record);
    for (int i = 0; i < schema->count; i++) {
        const AttributeSchema *attr = &schema->attributes[i];

        switch (attr->type) {
            case TYPE_INT:
                record->values[i].int_value = va_arg(args, int);
                break;
            case TYPE_FLOAT:
                record->values[i].float_value = (float)va_arg(args, double);
                break;
            case TYPE_CHAR: {
                const char *str = va_arg(args, char *);
                strncpy(record->values[i].string_value, str, attr->length);
                record->values[i].string_value[attr->length] = '\0';
                break;
            }
            default: break;
        }
    }
    va_end(args);
}


int record_get_key(const TableSchema *schema, const Record *record) {
    if (schema->key_index < 0) {
        printf("Error: No primary key defined in schema!\n");
        return -1;
    }

    if (schema->attributes[schema->key_index].type != TYPE_INT) {
        printf("Error: Primary key must be of type INT!\n");
        return -1;
    }

    return record->values[schema->key_index].int_value;
}

void schema_print(const TableSchema *schema){
    printf("Table Schema:\n");
    printf("  Attribute count: %d\n", schema->count);
    for (int i = 0; i < schema->count; i++) {
        const AttributeSchema *attr = &schema->attributes[i];
        printf("  %s ", attr->name);
        switch (attr->type) {
            case TYPE_INT:
                printf("INT");
                break;
            case TYPE_FLOAT:
                printf("FLOAT");
                break;
            case TYPE_CHAR:
                printf("CHAR(%d)", attr->length);
                break;
            default:
                printf("UNKNOWN");
                break;
        }
        if (i == schema->key_index) {
            printf(" PRIMARY KEY");
        }
        printf("\n");
    }
}

void record_print(const TableSchema *schema, const Record *record) {
    printf("(");
    for (int i = 0; i < schema->count; i++) {
        const AttributeSchema *attr = &schema->attributes[i];
        switch (attr->type) {
            case TYPE_INT:
                printf("%d", record->values[i].int_value);
                break;
            case TYPE_FLOAT:
                printf("%.2f", record->values[i].float_value);
                break;
            case TYPE_CHAR:
                printf("%s", record->values[i].string_value);
                break;
            default:
                printf("NULL");
                break;
        }
        if (i < schema->count - 1) printf(", ");
    }
    printf(")\n");
}

DataType get_type(const TableSchema *schema, const char *attr_name) {
    for (int i = 0; i < schema->count; i++) {
        if (strcmp(schema->attributes[i].name, attr_name) == 0) {
            const AttributeSchema *attr = &schema->attributes[i];
            switch (attr->type) {
                case TYPE_INT:
                    return TYPE_INT; // Success
                case TYPE_FLOAT:
                    return TYPE_FLOAT; // Success
                case TYPE_CHAR:
                    return TYPE_CHAR;
                default:
                    return TYPE_NULL;
            }
        }
    }
    return TYPE_NULL; // Attribute not found
}

DataType record_get_value(const TableSchema *schema, const Record *record, const char *attr_name, char *output) {
    for (int i = 0; i < schema->count; i++) {
        if (strcmp(schema->attributes[i].name, attr_name) == 0) {
            const AttributeSchema *attr = &schema->attributes[i];


            switch (attr->type) {
                case TYPE_INT: {
                    *(int *) output = record->values[i].int_value;
                    return TYPE_INT; // Success
                }case TYPE_FLOAT: {
                    *(float *) output = record->values[i].float_value;
                    return TYPE_FLOAT; // Success
                }case TYPE_CHAR: {
                    memcpy(output, record->values[i].string_value, attr->length);
                    return TYPE_CHAR; // Success
                }
                default: return TYPE_NULL;
            }
        }
    }
    return TYPE_NULL; // Attribute not found
}
