#include "../structs.h"

int temp_openDB (struct DB *db, const char *name, char **resolved);

int temp_create (const char *name, const char *query, const char **end_ptr);

void temp_dropAll ();

int temp_findTable (const char *name, char *filename);

void temp_setMappingDB (struct DB *db);

struct DB *temp_openMappingDB (const char *filename);
