#include "../structs.h"

int temp_openDB (struct DB *db, const char *filename);

int temp_create (const char *name, const char *query, const char **end_ptr);

int temp_dropAll ();
