#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../structs.h"
#include "parseNode.h"
#include "token.h"
#include "select.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"
#include "node.h"
#include "../debug.h"

#define MAX_CTES 10

/** WITH name AS (...) holding area — body points into the query string. */
struct PendingCte {
    char alias[MAX_TABLE_LENGTH];
    const char *body;
    size_t body_len;
};

static struct PendingCte *findCte(
    const char *table_name,
    struct PendingCte *ctes,
    int cte_count);

/**
 * Parse the interior of a parenthesized subquery into a heap Query*.
 * paren_len is from find_matching_parenthesis (includes both '(' and ')').
 */
static struct Query *parseParenSubquery(
    const char *query,
    size_t paren_index,
    int paren_len);

/* Defined below; also exported via parse.h */
struct Query *parseOwnedQuery(const char *sql);

/**
 * Move primary body (and any existing set_op) from q into a new heap Query.
 * Leaves q empty aside from FLAG_EXPLAIN. Caller owns the result.
 */
static struct Query *stealQueryBody(struct Query *q);

/** Attach a set-op Node on q with left/right arm Queries and op code. */
static int attachSetOp(
    struct Query *q,
    struct Query *left,
    struct Query *right,
    enum Function op);

/**
 * If a set-op left arm is pending, wrap current q body as the right arm using
 * *pending_op (UNION ALL / later UNION, INTERSECT, EXCEPT).
 * Returns 0 on success, -1 on error.
 */
static int flushPendingSetOp(
    struct Query *q,
    struct Query **pending_left,
    enum Function *pending_op);

int parseQuery(struct Query *q, const char *query, const char **end_ptr)
{
    /*********************
     * Begin Query parsing
     *********************/

    size_t index = 0;

    q->table_count = 0;

    q->predicate_count = 0;

    q->having_count = 0;
    q->having_nodes = NULL;

    q->order_count = 0;

    q->group_count = 0;

    q->distinct_count = 0;

    q->flags = 0;
    q->offset_value = 0;
    q->limit_value = -1;

    clearNode(&q->set_op);

    struct PendingCte ctes[MAX_CTES] = {0};
    int cte_count = 0;
    struct Query *pending_set_left = NULL;
    enum Function pending_set_op = FUNC_UNITY;

    skipWhitespace(query, &index);

    if (
        strncmp(query + index, "EXPLAIN", 7) == 0 && isspace(query[index + 7]))
    {
        q->flags |= FLAG_EXPLAIN;
        index += 8;
    }

    skipWhitespace(query, &index);

    // Special treatment for VALUES-only primary (also a UNION ALL arm).
    // Behaves as SELECT * FROM values_mem. Stops before UNION / ORDER / etc.
    if (strncmp(query + index, "VALUES", 6) == 0 &&
        (isspace((unsigned char)query[index + 6]) || query[index + 6] == '('))
    {
        index += 6;

        struct Table *table = allocateTable(q);

        skipWhitespace(query, &index);

        struct DB *db = calloc(1, sizeof(*db));
        if (db == NULL)
        {
            fprintf(stderr, "Out of memory\n");
            return -1;
        }

        const char *end = csvMem_fromValues(db, query + index, -1);
        index = (size_t)(end - query);

        table->db = db;
        strcpy(table->name, "VALUES");
        strcpy(table->alias, "values");

        skipWhitespace(query, &index);

        // Fall through only for set-op / presentation clauses on this statement.
        // Otherwise end the statement so multi-statement scripts keep working
        // (INSERT … VALUES (…); TABLE … must not parse TABLE here).
        int continue_clauses =
            (strncmp(query + index, "UNION", 5) == 0 &&
             !isalnum((unsigned char)query[index + 5])) ||
            (strncmp(query + index, "INTERSECT", 9) == 0 &&
             !isalnum((unsigned char)query[index + 9])) ||
            (strncmp(query + index, "EXCEPT", 6) == 0 &&
             !isalnum((unsigned char)query[index + 6])) ||
            (strncmp(query + index, "ORDER", 5) == 0 &&
             !isalnum((unsigned char)query[index + 5])) ||
            (strncmp(query + index, "LIMIT", 5) == 0 &&
             !isalnum((unsigned char)query[index + 5])) ||
            (strncmp(query + index, "OFFSET", 6) == 0 &&
             !isalnum((unsigned char)query[index + 6])) ||
            (strncmp(query + index, "FETCH", 5) == 0 &&
             !isalnum((unsigned char)query[index + 5]));

        if (!continue_clauses)
        {
            if (query[index] == ';')
            {
                index++;
            }
            if (end_ptr != NULL)
            {
                *end_ptr = query + index;
            }
            return 0;
        }
    }

    // Special treatment for TABLE query
    // Will behave as a query: SELECT * FROM table
    if (strncmp(query + index, "TABLE", 5) == 0 && isspace(query[index + 5]))
    {
        index += 6;

        char name[MAX_TABLE_LENGTH] = {0};

        getQuotedToken(query, &index, name, MAX_FIELD_LENGTH);

        if (name[0] == '\0')
        {
            fprintf(stderr, "expected a table name\n");
            return -1;
        }

        skipWhitespace(query, &index);

        if (query[index] != '\0' && query[index] != ';')
        {
            fprintf(
                stderr,
                "expected end of TABLE query. found '%s'\n",
                query + index);
            return -1;
        }

        if (query[index] == ';')
        {
            index++;
        }

        if (end_ptr != NULL)
        {
            *end_ptr = query + index;
        }

        char buffer[1024];
        sprintf(buffer, "FROM \"%s\"", name);

        return parseQuery(q, buffer, NULL);
    }

    char keyword[MAX_FIELD_LENGTH] = {0};

    while (query[index] != '\0' && query[index] != ';')
    {
        // Check for MySQL dump style comments
        if (query[index] == '/' && query[index + 1] == '*')
        {
            index += 2;
            while (query[index] != '\0' &&
                   !(query[index] == '*' && query[index + 1] == '/'))
            {
                index++;
            }
            if (query[index] == '*' && query[index + 1] == '/')
            {
                index += 2;
            }
        }

        if (query[index] == ';')
        {
            break;
        }

        int token_length = getToken(query, &index, keyword, MAX_FIELD_LENGTH);

        if (token_length == 0)
        {
            // This means it consumed whitespace then found the end of the input
            break;
        }

        if (token_length < 0)
        {
            fprintf(
                stderr,
                "Unable to get next token but not at end of query. Remaining: "
                "'%s'\n",
                query + index);
            return -1;
        }

        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "VALUES") == 0)
        {
            // Primary arm: SELECT * FROM in-memory VALUES rows.
            // csvMem_fromValues stops before UNION / ORDER / LIMIT / etc.
            struct Table *table = allocateTable(q);

            skipWhitespace(query, &index);

            struct DB *db = calloc(1, sizeof(*db));
            if (db == NULL)
            {
                fprintf(stderr, "Out of memory\n");
                if (pending_set_left != NULL)
                {
                    destroyQuery(pending_set_left);
                    free(pending_set_left);
                }
                return -1;
            }

            const char *values_start = query + index;
            const char *end = csvMem_fromValues(db, values_start, -1);
            /*
             * csvMem_fromValues consumes a trailing ';' when present. Detect
             * that before skipWhitespace — otherwise the next statement
             * (e.g. another VALUES … UNION …) is parsed as more of this arm
             * and set ops see a column-count mismatch.
             */
            int ate_semicolon = (end > values_start && *(end - 1) == ';');
            index = (size_t)(end - query);

            table->db = db;
            strcpy(table->name, "VALUES");
            strcpy(table->alias, "values");

            // Stop this statement so multi-statement scripts do not continue.
            skipWhitespace(query, &index);
            if (ate_semicolon || query[index] == '\0' || query[index] == ';')
            {
                break;
            }
        }
        else if (strcmp(keyword, "WITH") == 0)
        {
            while (query[index] != '\0' && query[index] != ';')
            {
                int cte_index = cte_count++;

                if (cte_index >= MAX_CTES)
                {
                    fprintf(
                        stderr,
                        "Cannot have more than %d ctes.\n",
                        MAX_CTES);
                    return -1;
                }

                struct PendingCte *cte = ctes + cte_index;

                getQuotedToken(query, &index, cte->alias, MAX_TABLE_LENGTH);

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                if (strcmp(keyword, "AS") != 0)
                {
                    fprintf(stderr, "expected AS\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                if (query[index] != '(')
                {
                    fprintf(stderr, "Expected '('\n");
                    return -1;
                }

                int len = find_matching_parenthesis(query + index);

                if (len < 2)
                {
                    fprintf(stderr, "Empty CTE\n");
                    return -1;
                }

                // Keep a pointer into the query text; parse into a Query* when
                // the CTE is referenced from FROM (avoids 255-byte name limit).
                cte->body = query + index + 1;
                cte->body_len = (size_t)(len - 2);

                index += len;

                skipWhitespace(query, &index);

                if (query[index] != ',')
                {
                    break;
                }

                index++;
            }
        }
        else if (strcmp(keyword, "SELECT") == 0)
        {
            skipWhitespace(query, &index);

            // SELECT DISTINCT [ON (...)] ...
            if (strncmp(query + index, "DISTINCT", 8) == 0 &&
                (isspace((unsigned char)query[index + 8]) ||
                 query[index + 8] == '(' ||
                 query[index + 8] == '\0'))
            {
                index += 8;
                q->flags |= FLAG_DISTINCT;

                skipWhitespace(query, &index);

                if (strncmp(query + index, "ON", 2) == 0 &&
                    (isspace((unsigned char)query[index + 2]) ||
                     query[index + 2] == '('))
                {
                    index += 2;
                    q->flags |= FLAG_DISTINCT_ON;

                    skipWhitespace(query, &index);

                    if (query[index] != '(')
                    {
                        fprintf(stderr, "Expected '(' after DISTINCT ON\n");
                        return -1;
                    }

                    index++;

                    while (query[index] != '\0' && query[index] != ';')
                    {
                        int i = q->distinct_count++;

                        if (i >= MAX_FIELD_COUNT)
                        {
                            fprintf(stderr, "Too many DISTINCT ON columns\n");
                            return -1;
                        }

                        skipWhitespace(query, &index);

                        int result = parseNode(
                            query,
                            &index,
                            &q->distinct_nodes[i],
                            q);
                        if (result < 0)
                        {
                            return result;
                        }

                        skipWhitespace(query, &index);

                        if (query[index] == ',')
                        {
                            index++;
                            continue;
                        }

                        break;
                    }

                    skipWhitespace(query, &index);

                    if (query[index] != ')')
                    {
                        fprintf(stderr, "Expected ')' after DISTINCT ON\n");
                        return -1;
                    }

                    index++;
                    skipWhitespace(query, &index);

                    if (q->distinct_count == 0)
                    {
                        fprintf(stderr, "DISTINCT ON requires at least one expression\n");
                        return -1;
                    }
                }
            }

            int curr_index = 0;
            while (query[index] != '\0' && query[index] != ';')
            {
                struct Node *node = allocateColumnNode(q);

                if (curr_index >= MAX_FIELD_COUNT + 1)
                {
                    fprintf(stderr, "Too many columns\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                int col_start_index = index;

                int result = parseNodeList(query, &index, node, q);

                if (result < 0)
                {
                    return result;
                }

                // Default alias is whole column spec (if it fits in)
                int len = index - col_start_index;

                // If it's a single double-quoted column
                if (query[col_start_index] == '"' && node->function == FUNC_UNITY)
                {
                    int end;
                    for (
                        end = col_start_index + len - 1;
                        end > col_start_index && query[end] != '"';
                        end--)
                    {
                    }

                    col_start_index++;
                    len = end - col_start_index;
                }

                if (len > 0)
                {
                    whitespaceCollapse(
                        node->alias,
                        query + col_start_index,
                        len < MAX_FIELD_LENGTH ? len : MAX_FIELD_LENGTH - 1);
                }

                q->flags |= result;

                skipWhitespace(query, &index);

                if (strncmp(query + index, "FILTER", 6) == 0)
                {
                    index += 6;

                    skipWhitespace(query, &index);

                    if (query[index] != '(')
                    {
                        fprintf(stderr, "Expected '(', got %c\n", query[index]);
                        return -1;
                    }

                    index++;

                    skipWhitespace(query, &index);

                    if (strncmp(query + index, "WHERE ", 6) != 0)
                    {
                        fprintf(stderr, "Expected 'WHERE', got %c\n", query[index]);
                        return -1;
                    }

                    index += 6;

                    skipWhitespace(query, &index);

                    node->filter = calloc(1, sizeof *node->filter);

                    int filter_result =
                        parseNodeList(query, &index, node->filter, q);
                    if (filter_result < 0)
                    {
                        return filter_result;
                    }

                    if (query[index] != ')')
                    {
                        fprintf(stderr, "Expected ')', got %c\n", query[index]);
                        return -1;
                    }

                    index++;

                    skipWhitespace(query, &index);
                }

                if (strncmp(query + index, "AS ", 3) == 0)
                {
                    index += 3;

                    getQuotedToken(
                        query,
                        &index,
                        node->alias,
                        MAX_FIELD_LENGTH);

                    if (node->alias[0] == '\'')
                    {
                        fprintf(stderr, "Don't use single quote for alias names. Found: %s\n", node->alias);
                    }

                    skipWhitespace(query, &index);
                }

                if (query[index] != ',')
                {
                    break;
                }

                index++;
            }
        }
        else if (strcmp(keyword, "FROM") == 0)
        {
            int next_join_flag = 0;
            /* CROSS JOIN must not take ON/USING (standard SQL). */
            int next_is_cross = 0;

            while (query[index] != '\0' && query[index] != ';')
            {
                struct Table *table = allocateTable(q);

                table->join.function = OPERATOR_ALWAYS;
                table->join_type = next_join_flag;
                int is_cross = next_is_cross;
                next_join_flag = 0;
                next_is_cross = 0;

                skipWhitespace(query, &index);

                if (query[index] == '(')
                {
                    // Subquery time!

                    int len = find_matching_parenthesis(query + index);

                    if (strncmp(query + index + 1, "VALUES", 6) == 0)
                    {
                        // VALUES subqueries can be handled in process
                        // We'll construct a temp DB to hold data until
                        // populateTables().

                        const char *end_ptr = query + index + len;

                        index += strlen("(VALUES");

                        skipWhitespace(query, &index);

                        // Malloc a DB ahead of time. It will be copied to the
                        // stack in populateTables() then populateTables() will
                        // free this for us.
                        struct DB *db = calloc(1, sizeof(*db));

                        csvMem_fromValues(
                            db,
                            query + index,
                            end_ptr - query - index);

                        // This DB needs to be free'd in populateTables()
                        table->db = db;

                        strcpy(table->name, "VALUES");

                        index = end_ptr - query;
                    }
                    else
                    {
                        struct Query *sq = parseParenSubquery(query, index, len);
                        if (sq == NULL)
                        {
                            return -1;
                        }

                        table->subquery = sq;
                        table->db = DB_SUBQUERY;
                        strcpy(table->name, "SUBQUERY");

                        index += len;

                        strcpy(table->alias, "subquery");

                        // TODO: subqueries can't reference CTEs
                    }
                }
                else
                {
                    getQuotedToken(
                        query,
                        &index,
                        table->name,
                        MAX_TABLE_LENGTH);

                    if (query[index] == '(')
                    {
                        // we have a table-valued-function

                        int len = find_matching_parenthesis(query + index);
                        int table_len = strlen(table->name);

                        strncpy(table->name + table_len, query + index, len);
                        table->name[table_len + len] = '\0';

                        index += len;
                    }
                    else
                    {
                        // We just have a regular table identifier

                        // As soon as we have a name we should search for a
                        // matching CTE
                        struct PendingCte *cte = findCte(
                            table->name,
                            ctes,
                            cte_count);

                        if (cte != NULL)
                        {
                            strcpy(table->alias, cte->alias);

                            // VALUES CTEs match FROM (VALUES …): build the mem
                            // DB now. parseOwnedQuery would attach a live DB
                            // that cloneQuery cannot copy on materialize.
                            if (cte->body_len >= 6 &&
                                strncmp(cte->body, "VALUES", 6) == 0 &&
                                (cte->body_len == 6 ||
                                 isspace((unsigned char)cte->body[6]) ||
                                 cte->body[6] == '('))
                            {
                                size_t v = 6;
                                while (v < cte->body_len &&
                                       isspace((unsigned char)cte->body[v]))
                                {
                                    v++;
                                }

                                struct DB *db = calloc(1, sizeof(*db));
                                if (db == NULL)
                                {
                                    fprintf(stderr, "Out of memory\n");
                                    return -1;
                                }

                                // Null-terminated copy: csvMem_fromValues(-1)
                                // walks until '\0' (same as top-level VALUES).
                                char *vals = malloc(cte->body_len - v + 1);
                                if (vals == NULL)
                                {
                                    free(db);
                                    fprintf(stderr, "Out of memory\n");
                                    return -1;
                                }
                                memcpy(vals, cte->body + v, cte->body_len - v);
                                vals[cte->body_len - v] = '\0';
                                csvMem_fromValues(db, vals, -1);
                                free(vals);

                                table->db = db;
                                strcpy(table->name, "VALUES");
                            }
                            else
                            {
                                // Parse CTE body into a heap Query for this
                                // table. Each FROM reference gets its own
                                // Query* (same as historically re-parsing).
                                char *sql = malloc(cte->body_len + 1);
                                if (sql == NULL)
                                {
                                    fprintf(stderr, "Out of memory\n");
                                    return -1;
                                }
                                memcpy(sql, cte->body, cte->body_len);
                                sql[cte->body_len] = '\0';

                                struct Query *sq = parseOwnedQuery(sql);
                                free(sql);
                                if (sq == NULL)
                                {
                                    return -1;
                                }

                                table->subquery = sq;
                                table->db = DB_SUBQUERY;
                                strcpy(table->name, "SUBQUERY");
                            }

                            // TODO: CTEs can't reference CTEs
                        }
                    }
                }

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0)
                {
                    index += 3;

                    getQuotedToken(
                        query,
                        &index,
                        table->alias,
                        MAX_FIELD_LENGTH);

                    skipWhitespace(query, &index);

                    // Check for column aliases
                    if (query[index] == '(')
                    {

                        index++;

                        // Stored in table alias struct member.
                        // [table_alias + 2] will be an opening parenthesis as a
                        // sentinal value then column aliases will follow as
                        // null separated values. e.g.:
                        //
                        //  table alias\0(col 1\0col 2\0

                        int alias_len = strlen(table->alias);

                        char *d = table->alias + alias_len + 1;

                        *(d++) = '(';

                        char token[MAX_FIELD_LENGTH];

                        while (query[index] != '\0' && query[index] != ';')
                        {
                            getQuotedToken(query, &index, token, MAX_FIELD_LENGTH);

                            strcpy(d, token);

                            d += strlen(token) + 1;

                            skipWhitespace(query, &index);

                            char c = query[index];

                            if (query[index] == ')')
                            {
                                // We're done with the column aliases
                                index++;
                                break;
                            }

                            if (c != ',')
                            {
                                // We expect a comma in order to continue with
                                // the loop, otherwise break.
                                break;
                            }

                            index++;
                        }
                    }
                }
                // Alias might already have been set e.g. if coming from a CTE
                else if (table->alias[0] == '\0')
                {
                    strcpy(table->alias, table->name);
                }

                skipWhitespace(query, &index);

                // Parse JOIN predicate
                if (strncmp(query + index, "ON ", 3) == 0)
                {
                    if (is_cross)
                    {
                        fprintf(
                            stderr,
                            "CROSS JOIN cannot have ON or USING; "
                            "use INNER JOIN\n");
                        return -1;
                    }

                    index += 3;
                    skipWhitespace(query, &index);

                    struct Node *p = &table->join;

                    int join_result = parseNodeList(query, &index, p, q);
                    if (join_result < 0)
                    {
                        return join_result;
                    }

                    skipWhitespace(query, &index);
                }
                else if (strncmp(query + index, "USING ", 6) == 0)
                {
                    if (is_cross)
                    {
                        fprintf(
                            stderr,
                            "CROSS JOIN cannot have ON or USING; "
                            "use INNER JOIN\n");
                        return -1;
                    }

                    index += 6;
                    skipWhitespace(query, &index);

                    struct Node *p = &table->join;

                    p->children = malloc(sizeof(*p) * 2);
                    p->child_count = 2;

                    struct Node *left = &p->children[0];
                    struct Node *right = &p->children[1];

                    int result = parseNode(query, &index, left, q);
                    if (result < 0)
                    {
                        fprintf(stderr, "Unable to parse USING node\n");
                        return result;
                    }

                    // Duplicate node from left to right side of predicate
                    copyNodeTree(right, left);

                    // One side (right) needs to be on this joined table
                    // The other side needs to be from any of the previous
                    // tables we don't which yet, but it will be filled in later
                    if (right->child_count == -1)
                    {
                        right->field.table_id = q->table_count - 1;
                    }
                    else if (right->child_count > 0)
                    {
                        right->children[0].field.table_id = q->table_count - 1;
                    }

                    // Set operator
                    p->function = OPERATOR_EQ;
                }
                else
                {
                    table->join.function = OPERATOR_ALWAYS;
                }

                // A RIGHT JOIN B ≡ B LEFT JOIN A. Swap while table_ids in
                // SELECT/WHERE are still unresolved (by name/alias later).
                if (table->join_type == JOIN_RIGHT)
                {
                    if (q->table_count != 2)
                    {
                        fprintf(
                            stderr,
                            "RIGHT JOIN only supported with a single table on "
                            "the left; rewrite as LEFT JOIN\n");
                        return -1;
                    }

                    struct Table left = q->tables[0];
                    struct Table right = q->tables[1];

                    // Preserve join predicate/type on the inner (ex-left) side.
                    q->tables[0] = right;
                    q->tables[0].join = left.join;
                    q->tables[0].join_type = JOIN_INNER;

                    q->tables[1] = left;
                    q->tables[1].join = right.join;
                    q->tables[1].join_type = JOIN_LEFT;
                }

                if (query[index] == ',')
                {
                    index++;

                    // loop again
                }
                else
                {
                    int old_index = index;
                    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                    if (strcmp(keyword, "INNER") == 0)
                    {
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);
                    }
                    else if (strcmp(keyword, "CROSS") == 0)
                    {
                        // CROSS JOIN: Cartesian product only (no ON/USING).
                        next_is_cross = 1;
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);
                    }
                    else if (strcmp(keyword, "LEFT") == 0)
                    {
                        next_join_flag = JOIN_LEFT;
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                        // Optional OUTER keyword: LEFT OUTER JOIN
                        if (strcmp(keyword, "OUTER") == 0)
                        {
                            getToken(
                                query, &index, keyword, MAX_FIELD_LENGTH);
                        }
                    }
                    else if (strcmp(keyword, "RIGHT") == 0)
                    {
                        next_join_flag = JOIN_RIGHT;
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                        // Optional OUTER keyword: RIGHT OUTER JOIN
                        if (strcmp(keyword, "OUTER") == 0)
                        {
                            getToken(
                                query, &index, keyword, MAX_FIELD_LENGTH);
                        }
                    }
                    else if (strcmp(keyword, "FULL") == 0)
                    {
                        fprintf(
                            stderr,
                            "FULL OUTER JOIN is not supported\n");
                        return -1;
                    }

                    if (strcmp(keyword, "JOIN") == 0)
                    {
                        // loop again
                    }
                    else
                    {
                        // Rewind then break out of FROM clause
                        index = old_index;
                        break;
                    }
                }
            }
        }
        else if (strcmp(keyword, "WHERE") == 0)
        {
            q->flags |= FLAG_HAVE_PREDICATE;

            struct Node *p = allocatePredicateNode(q);

            int result = parseNodeList(query, &index, p, q);
            if (result < 0)
            {
                return result;
            }

            if ((p->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR)
            {
                fprintf(stderr, "WHERE node: expected =|<|<=|>|>=\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "HAVING") == 0)
        {
            // HAVING implies an aggregation context even without GROUP BY
            q->flags |= FLAG_GROUP;

            struct Node *p = allocateHavingNode(q);

            int result = parseNodeList(query, &index, p, q);
            if (result < 0)
            {
                return result;
            }

            if ((p->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR)
            {
                fprintf(stderr, "HAVING node: expected =|<|<=|>|>=\n");
                return -1;
            }
        }
        else if (
            strcmp(keyword, "UNION") == 0 ||
            strcmp(keyword, "INTERSECT") == 0 ||
            strcmp(keyword, "EXCEPT") == 0)
        {
            enum Function set_op = FUNC_UNION;
            if (strcmp(keyword, "INTERSECT") == 0)
            {
                set_op = FUNC_INTERSECT;
            }
            else if (strcmp(keyword, "EXCEPT") == 0)
            {
                set_op = FUNC_EXCEPT;
            }

            size_t after_keyword = index;
            getToken(query, &index, keyword, MAX_FIELD_LENGTH);
            if (strcmp(keyword, "ALL") == 0)
            {
                if (set_op == FUNC_UNION)
                {
                    set_op = FUNC_UNION_ALL;
                }
                else
                {
                    fprintf(
                        stderr,
                        "%s ALL is not supported\n",
                        set_op == FUNC_INTERSECT ? "INTERSECT" : "EXCEPT");
                    if (pending_set_left != NULL)
                    {
                        destroyQuery(pending_set_left);
                        free(pending_set_left);
                    }
                    return -1;
                }
            }
            else if (strcmp(keyword, "DISTINCT") == 0)
            {
                // Explicit DISTINCT is the default for set ops.
            }
            else
            {
                // No qualifier — leave index before this token.
                index = after_keyword;
            }

            // Left-assoc: if a previous arm is pending, current q is that
            // arm's right — wrap before starting the next arm.
            if (pending_set_left != NULL)
            {
                if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
                {
                    return -1;
                }
            }

            pending_set_left = stealQueryBody(q);
            if (pending_set_left == NULL)
            {
                fprintf(stderr, "Out of memory\n");
                return -1;
            }
            pending_set_op = set_op;
        }
        else if (strcmp(keyword, "OFFSET") == 0)
        {
            if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
            {
                return -1;
            }
            q->offset_value = getNumericToken(query, &index);

            if (q->offset_value < 0)
            {
                fprintf(stderr, "OFFSET cannot be negative\n");
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0)
            {
                fprintf(stderr, "expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "FETCH") == 0)
        {
            if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
            {
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0)
            {
                fprintf(stderr, "expected FIRST|NEXT\n");
                return -1;
            }

            skipWhitespace(query, &index);

            if (isdigit(query[index]))
            {

                q->limit_value = getNumericToken(query, &index);

                if (q->limit_value < 0)
                {
                    fprintf(stderr, "FETCH FIRST cannot be negative\n");
                    return -1;
                }
            }
            else
            {
                q->limit_value = 1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0)
            {
                fprintf(stderr, "expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ONLY") != 0)
            {
                fprintf(stderr, "expected ONLY; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "LIMIT") == 0)
        {
            if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
            {
                return -1;
            }

            q->limit_value = getNumericToken(query, &index);

            if (q->limit_value < 0)
            {
                fprintf(stderr, "LIMIT cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "ORDER") == 0)
        {
            if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
            {
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "BY") != 0)
            {
                fprintf(stderr, "expected BY\n");
                return -1;
            }

            while (query[index] != '\0' && query[index] != ';')
            {
                int i = q->order_count++;

                skipWhitespace(query, &index);

                int result = parseNode(query, &index, &q->order_nodes[i], q);
                if (result < 0)
                {
                    return -1;
                }

                if (
                    strcmp(q->order_nodes[0].field.text, "PK") == 0 && query[index] == '(')
                {
                    // We've been asked to sort on primary key.
                    // We don't actually care which column it is so we just
                    // discard the contents of the parentheses.
                    int len = find_matching_parenthesis(query + index);
                    index += len;
                }

                size_t original_index = index;

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                if (strcmp(keyword, "ASC") == 0)
                {
                    q->order_nodes[i].alias[0] = ORDER_ASC;
                }
                else if (strcmp(keyword, "DESC") == 0)
                {
                    q->order_nodes[i].alias[0] = ORDER_DESC;
                }
                else
                {
                    q->order_nodes[i].alias[0] = ORDER_ASC;
                    // backtrack
                    index = original_index;
                }

                skipWhitespace(query, &index);

                if (query[index] != ',')
                {
                    break;
                }

                index++;
            }
        }
        else if (strcmp(keyword, "GROUP") == 0)
        {
            if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
            {
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "BY") != 0)
            {
                fprintf(stderr, "expected BY\n");
                return -1;
            }

            q->flags |= FLAG_GROUP;

            while (query[index] != '\0' && query[index] != ';')
            {
                int i = q->group_count++;

                skipWhitespace(query, &index);

                int result = parseNode(query, &index, &q->group_nodes[i], q);
                if (result < 0)
                {
                    return -1;
                }

                skipWhitespace(query, &index);

                if (query[index] != ',')
                {
                    break;
                }

                index++;
            }
        }
        else
        {
            fprintf(
                stderr,
                "expected WITH|VALUES|SELECT|FROM|WHERE|HAVING|OFFSET|FETCH "
                "FIRST|LIMIT|ORDER|GROUP|UNION|INTERSECT|EXCEPT\n");
            fprintf(stderr, "Found '%s'\n", keyword);
            if (pending_set_left != NULL)
            {
                destroyQuery(pending_set_left);
                free(pending_set_left);
            }
            return -1;
        }
    }

    if (flushPendingSetOp(q, &pending_set_left, &pending_set_op) < 0)
    {
        return -1;
    }

    // Consume trailing semicolon
    if (query[index] == ';')
    {
        index++;
    }

    if (end_ptr != NULL)
    {
        *end_ptr = &query[index];
    }

    return 0;
}

static struct PendingCte *findCte(
    const char *table_name,
    struct PendingCte *ctes,
    int cte_count)
{
    for (int i = 0; i < cte_count; i++)
    {
        if (strcmp(ctes[i].alias, table_name) == 0)
        {
            return &ctes[i];
        }
    }

    return NULL;
}

/**
 * Shallow-move a Node (including owned pointers) from src to dest, then zero src
 * without freeing — ownership transfers to dest.
 */
static void moveNode(struct Node *dest, struct Node *src)
{
    *dest = *src;
    memset(src, 0, sizeof(*src));
    src->function = FUNC_UNITY;
    src->field.index = FIELD_UNKNOWN;
    src->field.table_id = -1;
}

static struct Query *stealQueryBody(struct Query *q)
{
    struct Query *left = calloc(1, sizeof(*left));
    if (left == NULL)
    {
        return NULL;
    }

#ifdef DEBUG
    left->id = q->id;
#endif

    // Keep EXPLAIN on the outermost query only.
    left->flags = q->flags & ~FLAG_EXPLAIN;
    q->flags &= FLAG_EXPLAIN;

    left->tables = q->tables;
    left->table_count = q->table_count;
    q->tables = NULL;
    q->table_count = 0;

    left->column_nodes = q->column_nodes;
    left->column_count = q->column_count;
    q->column_nodes = NULL;
    q->column_count = 0;

    left->predicate_nodes = q->predicate_nodes;
    left->predicate_count = q->predicate_count;
    q->predicate_nodes = NULL;
    q->predicate_count = 0;

    left->having_nodes = q->having_nodes;
    left->having_count = q->having_count;
    q->having_nodes = NULL;
    q->having_count = 0;

    left->order_count = q->order_count;
    for (int i = 0; i < q->order_count; i++)
    {
        moveNode(&left->order_nodes[i], &q->order_nodes[i]);
    }
    q->order_count = 0;

    left->group_count = q->group_count;
    for (int i = 0; i < q->group_count; i++)
    {
        moveNode(&left->group_nodes[i], &q->group_nodes[i]);
    }
    q->group_count = 0;

    left->distinct_count = q->distinct_count;
    for (int i = 0; i < q->distinct_count; i++)
    {
        moveNode(&left->distinct_nodes[i], &q->distinct_nodes[i]);
    }
    q->distinct_count = 0;

    left->offset_value = q->offset_value;
    left->limit_value = q->limit_value;
    q->offset_value = 0;
    q->limit_value = -1;

    moveNode(&left->set_op, &q->set_op);

    return left;
}

static int attachSetOp(
    struct Query *q,
    struct Query *left,
    struct Query *right,
    enum Function op)
{
    if (left == NULL || right == NULL)
    {
        return -1;
    }

    // q should not already hold a set_op with children.
    if (q->set_op.child_count > 0 || q->set_op.subquery != NULL)
    {
        freeNode(&q->set_op);
    }
    clearNode(&q->set_op);

    allocateNodeChildren(&q->set_op, 2);
    q->set_op.function = op;
    q->set_op.children[0].subquery = left;
    q->set_op.children[1].subquery = right;

    return 0;
}

static int flushPendingSetOp(
    struct Query *q,
    struct Query **pending_left,
    enum Function *pending_op)
{
    if (pending_left == NULL || *pending_left == NULL)
    {
        return 0;
    }

    enum Function op =
        (pending_op != NULL && *pending_op != FUNC_UNITY)
            ? *pending_op
            : FUNC_UNION_ALL;

    struct Query *right = stealQueryBody(q);
    if (right == NULL)
    {
        destroyQuery(*pending_left);
        free(*pending_left);
        *pending_left = NULL;
        if (pending_op != NULL)
        {
            *pending_op = FUNC_UNITY;
        }
        fprintf(stderr, "Out of memory\n");
        return -1;
    }

    if (attachSetOp(q, *pending_left, right, op) < 0)
    {
        destroyQuery(*pending_left);
        free(*pending_left);
        destroyQuery(right);
        free(right);
        *pending_left = NULL;
        if (pending_op != NULL)
        {
            *pending_op = FUNC_UNITY;
        }
        return -1;
    }

    *pending_left = NULL;
    if (pending_op != NULL)
    {
        *pending_op = FUNC_UNITY;
    }
    return 0;
}

struct Query *parseOwnedQuery(const char *sql)
{
    if (sql == NULL)
    {
        return NULL;
    }

    struct Query *q = calloc(1, sizeof(*q));
    if (q == NULL)
    {
        fprintf(stderr, "Out of memory\n");
        return NULL;
    }

    if (parseQuery(q, sql, NULL) < 0)
    {
        destroyQuery(q);
        free(q);
        return NULL;
    }

    return q;
}

static struct Query *parseParenSubquery(
    const char *query,
    size_t paren_index,
    int paren_len)
{
    if (paren_len < 2)
    {
        fprintf(stderr, "Empty subquery\n");
        return NULL;
    }

    size_t body_len = (size_t)(paren_len - 2);
    char *sql = malloc(body_len + 1);
    if (sql == NULL)
    {
        fprintf(stderr, "Out of memory\n");
        return NULL;
    }

    memcpy(sql, query + paren_index + 1, body_len);
    sql[body_len] = '\0';

    struct Query *sq = parseOwnedQuery(sql);
    free(sql);
    return sq;
}