struct DB {
    FILE *file;
    long *line_indices;
    int record_count;
};

void makeDB (struct DB *db, FILE *f);

int openDB (struct DB *db, const char *filename);

/**
 * Max header length 1024 characters
 */
int getFieldIndex (struct DB *db, const char *field);

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);
