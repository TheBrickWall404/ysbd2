// Created by theofilos on 10/30/25.
//
#include "record_generator.h"
#include <stdlib.h>


const char *names[] = {
    "Alexandros", // Αλέξανδρος
    "Sofia", // Σοφία
    "Dimitris", // Δημήτρης
    "Anna", // Αννα
    "Konstantinos", // Κωνσταντίνος
    "Maria", // Μαρία
    "Georgios", // Γεώργιος
    "Eleni", // Ελένη
    "Petros", // Πέτρος
    "Evangelia" // Ευαγγελία
};


const char *surnames[] = {
    "Papadopoulos", // Παπαδόπουλος
    "Georgiou", // Γεωργίου
    "Dimitriou", // Δημητρίου
    "Anagnostopoulos", // Αναγνωστόπουλος
    "Karagiannis", // Καραγιάννης
    "Mavromatis", // Μαυρομάτης
    "Nikolaou", // Νικολάου
    "Christodoulou", // Χριστοδούλου
    "Kostopoulos", // Κωστόπουλος
    "Stamatopoulos" // Σταματόπουλος
};

const char *cities[] = {
    "Athina", // Αθήνα
    "Patra", // Πάτρα
    "Irakleio", // Ηράκλειο
    "Larisa", // Λάρισα
    "Volos", // Βόλος
    "Ioannina", // Ιωάννινα
    "Chania", // Χανιά
    "Kalamata", // Καλαμάτα
    "Rodos" // Ρόδος
};

const char *universities[] = {
    "EKPA",        // ΕΚΠΑ - Εθνικό και Καποδιστριακό Πανεπιστήμιο Αθηνών
    "AUTH",        // ΑΠΘ - Αριστοτέλειο Πανεπιστήμιο Θεσσαλονίκης
    "PATRAS",      // Πανεπιστήμιο Πατρών
    "UOC",         // Πανεπιστήμιο Κρήτης
    "UOI",         // Πανεπιστήμιο Ιωαννίνων
    "UPATRAS",     // Πανεπιστήμιο Δυτικής Αττικής (formerly TEI Athinas)
    "AEGEAN",      // Πανεπιστήμιο Αιγαίου
    "UTH",         // Πανεπιστήμιο Θεσσαλίας
    "UOP",         // Πανεπιστήμιο Πελοποννήσου
    "UOWM"         // Πανεπιστήμιο Δυτικής Μακεδονίας
};

const char *departments[] = {
    "CS",        // Computer Science - Τμήμα Πληροφορικής
    "ECE",       // Electrical and Computer Engineering - ΗΜΜΥ
    "MECH",      // Mechanical Engineering - Μηχανολόγων Μηχανικών
    "CIVIL",     // Civil Engineering - Πολιτικών Μηχανικών
    "MED",       // Medicine - Ιατρική
    "LAW",       // Law - Νομική
    "ECON",      // Economics - Οικονομικών
    "PHARM",     // Pharmacy - Φαρμακευτική
    "PHYS",      // Physics - Φυσική
    "MATH"       // Mathematics - Μαθηματικά
};

int get_random_number(const int max) {
    return rand() % max;
}

TableSchema employee_get_schema() {
    const AttributeSchema attrs[] = {
        {"id", TYPE_INT, 0},
        {"name", TYPE_CHAR, 20},
        {"surname", TYPE_CHAR, 20},
        {"city", TYPE_CHAR, 20}
    };
    // Declare schema
    TableSchema schema;
    // Initialize schema with "id" as primary key
    schema_init(&schema, attrs, 4, "id");
    return schema;
}

TableSchema student_get_schema() {
    const AttributeSchema attrs[] = {
        {"id", TYPE_INT, 0},
        {"name", TYPE_CHAR, 20},
        {"surname", TYPE_CHAR, 20},
        {"university", TYPE_CHAR, 20},
        {"department", TYPE_CHAR, 20}
    };
    // Declare schema
    TableSchema schema;
    // Initialize schema with "id" as primary key
    schema_init(&schema, attrs, 5, "id");
    return schema;
}

void employee_random_record(const TableSchema *schema, Record *record) {\
    const int id = get_random_number(200000);
    int r = get_random_number(sizeof(names) / sizeof(names[0]));
    const char *name = names[r];
    r = get_random_number(sizeof(surnames) / sizeof(surnames[0]));
    const char *surname = surnames[r];
    r = get_random_number(sizeof(cities) / sizeof(cities[0]));
    const char *city = cities[r];
    record_create(schema, record,
                  id,  name,
                  surname, city
    );
}

void student_random_record(const TableSchema *schema, Record *record) {
    const int id = get_random_number(200000);
    int r = get_random_number(sizeof(names) / sizeof(names[0]));
    const char *name = names[r];
    r = get_random_number(sizeof(surnames) / sizeof(surnames[0]));
    const char *surname = surnames[r];
    r = get_random_number(sizeof(universities) / sizeof(universities[0]));
    const char *university = universities[r];
    r = get_random_number(sizeof(departments) / sizeof(departments[0]));
    const char *department = departments[r];
    record_create(schema, record,
                  id,  name,
                  surname, university,department
    );
}
