#include <string.h>

#include "../structs.h"
#include "../db/db.h"

// Takes column aliases into account before going down to db field if necessary
int getTableFieldIndex(struct Table *table, const char *field)
{
    int alias_len = strlen(table->alias);

    if (table->alias[alias_len + 1] == '(')
    {
        // Data format for column aliases:
        //
        //  table alias\0(col 1\0col 2\0

        char *curr_field = table->alias + alias_len + 2;

        for (int i = 0; i < table->db->field_count; i++)
        {
            if (strcmp(field, curr_field) == 0)
            {
                return i;
            }

            curr_field += strlen(curr_field) + 1;
        }

        return -1;
    }

    return getFieldIndex(table->db, field);
}

// Takes column aliases into account before going down to db field if necessary
char *getTableFieldName(struct Table *table, int field_index)
{
    int alias_len = strlen(table->alias);

    if (table->alias[alias_len + 1] == '(')
    {
        // Data format for column aliases:
        //
        //  table alias\0(col 1\0col 2\0

        char *curr_field = table->alias + alias_len + 2;

        for (int i = 0; i < table->db->field_count; i++)
        {
            if (i == field_index)
            {
                return curr_field;
            }

            curr_field += strlen(curr_field) + 1;
        }

        return "\0";
    }

    return getFieldName(table->db, field_index);
}