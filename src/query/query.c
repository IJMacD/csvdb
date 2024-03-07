#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/time.h>
#include <unistd.h>

#include "query.h"
#include "select.h"
#include "../execute/execute.h"
#include "../evaluate/evaluate.h"
#include "create.h"
#include "token.h"
#include "parse.h"
#include "explain.h"
#include "plan.h"
#include "optimise.h"
#include "../db/db.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"
#include "node.h"
#include "../debug.h"
#include "check.h"

/**
 * returns process exit code; negative for error
 */
int runQueries(
    const char *query_string,
    enum OutputOption output_flags,
    FILE *output)
{
    const char *end_ptr = query_string;

    while (*end_ptr != '\0')
    {
        // Skip leading whitespace
        while (*end_ptr != '\0' && isspace(*end_ptr))
        {
            end_ptr++;
        }

        // There wasn't actually another query
        if (*end_ptr == '\0')
        {
            return 0;
        }

        // Save start position
        const char *query_start = end_ptr;
        int result = query(query_start, output_flags, output, &end_ptr);

        if (result < 0)
        {
            if (query_start != end_ptr)
            {
                size_t query_len = end_ptr - query_start;
                fprintf(stderr, "Error with query: \n");
                fwrite(query_start, 1, query_len, stderr);
                fprintf(stderr, "\n");
            }

            return result;
        }

        if (query_start == end_ptr)
        {
            break;
        }
    }

    return 0;
}

/**
 * returns process exit code; negative for error
 */
int query(
    const char *query,
    enum OutputOption output_flags,
    FILE *output,
    const char **end_ptr)
{
    skipWhitespacePtr(&query);

    if (strncmp(query, "CREATE ", 7) == 0)
    {
        if (output_flags & FLAG_READ_ONLY)
        {
            fprintf(stderr, "Tried to CREATE while in read-only mode\n");
            return -1;
        }

        if (output_flags & FLAG_EXPLAIN)
        {
            return -1;
        }

        return create_query(query, end_ptr);
    }

    if (strncmp(query, "INSERT ", 7) == 0)
    {
        if (output_flags & FLAG_READ_ONLY)
        {
            fprintf(stderr, "Tried to INSERT while in read-only mode\n");
            return -1;
        }

        if (output_flags & FLAG_EXPLAIN)
        {
            return 0;
        }

        return insert_query(query, end_ptr);
    }

    if (strncmp(query, "DROP ", 5) == 0)
    {
        if (output_flags & FLAG_READ_ONLY)
        {
            fprintf(stderr, "Tried to DROP while in read-only mode\n");
            return -1;
        }

        if (output_flags & FLAG_EXPLAIN)
        {
            return 0;
        }

        while (*query != ';' && *query != '\0')
            query++;

        *end_ptr = query;

        // return drop_query(query, end_ptr);
        return -1;
    }

    if (strncmp(query, "LOCK ", 5) == 0)
    {
        // NO OP

        while (*query != ';' && *query != '\0')
            query++;

        *end_ptr = query;

        return 0;
    }

    // If we're querying the stats table then we must have stats turned off
    // for this query otherwise they would get overwritten.
    // Note pretty finicky and defeated by whitespace.
    if (
        strcmp(query, "TABLE stats") == 0 || strstr(query, "FROM stats") != NULL)
    {
        output_flags &= ~OUTPUT_OPTION_STATS;
    }

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        // start stats file
        FILE *fstats = fopen("stats.csv", "w");
        if (fstats)
        {
            fputs("operation,duration\n", fstats);
            fclose(fstats);
        }
    }

    return select_query(query, output_flags, output, end_ptr);
}
