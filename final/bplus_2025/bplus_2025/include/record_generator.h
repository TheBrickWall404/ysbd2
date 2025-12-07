//
// Created by theofilos on 10/30/25.
//


#ifndef BPLUS_EMPLOYEE_H
#define BPLUS_EMPLOYEE_H
#include <record.h>

TableSchema employee_get_schema();
TableSchema student_get_schema();

void employee_random_record(const TableSchema* schema, Record *record);
void student_random_record(const TableSchema *schema, Record *record);

#endif //BPLUS_EMPLOYEE_H