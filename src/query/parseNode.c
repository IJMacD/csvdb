#include <string.h>

#include "../structs.h"
#include "token.h"
#include "node.h"
#include "select.h"
#include "../functions/util.h"

#include "parseNode.h"

static struct Node *constructExpressionTree(
    struct Node *node,
    enum Function next_function);

static int parseString(char *value, struct Node *node);

static int parseFunction(const char *value, const char *query, size_t *index, struct Node *node, struct Query *q);

static int parseFunctionParams(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q);

static int parseOperator(const char *input);

static int getPrecedence(enum Function fn);

static enum Function parseSimpleOperators(const char *query, size_t *index);

/**
 * Parses simple nodes:
 *  Simple nodes: (<node>)
 *      <field>
 *      <constant>
 *      <function>()
 *      <function>(<node>)
 *      etc.
 * Returns -1 for error; flags otherwise
 */
int parseSimpleNode(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q)
{
    char value[MAX_FIELD_LENGTH];

    clearNode(node);

    if (query[*index] == '*')
    {
        node->field.index = FIELD_STAR;

        // '*' will default to ALL tables
        node->field.table_id = -1;

        (*index)++;

        return 0;
    }

    int quoted_flag = getQuotedToken(query, index, value, MAX_FIELD_LENGTH);

    if (quoted_flag == 2)
    {
        // Field is explicit, it can't be a function or special column name
        strcpy(node->field.text, value);

        // Whether we found a simple operator or not, we're done here

        return 0;
    }

    if (strcmp(value, "ROW_NUMBER") == 0)
    {
        if (query[*index] != '(' && query[(*index) + 1] != ')')
        {
            fprintf(stderr, "Expected () after ROW_NUMBER\n");
            return -1;
        }
        (*index) += 2;

        node->field.index = FIELD_ROW_NUMBER;

        return 0;
    }

    if (strcmp(value, "rowid") == 0)
    {
        node->field.index = FIELD_ROW_INDEX;

        // default to first table
        node->field.table_id = 0;

        return 0;
    }

    if (value[0] == '\0')
    {
        fprintf(stderr, "Error in parsing: Unexpected end of query\n");
        return -1;
    }

    if (query[*index] == '(')
    {
        (*index)++;
        // We have a function

        return parseFunction(value, query, index, node, q);
    }

    if (value[0] == '\'')
    {
        return parseString(value, node);
    }

    if (is_numeric(value))
    {
        // Detected numeric constant
        strcpy(node->field.text, value);
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
        return 0;
    }

    if (value[0] == '0' && value[1] == 'x')
    {
        // Detected hex numeric constant
        strcpy(node->field.text, value);
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
        return 0;
    }

    // Just a regular bare field or named constant
    strcpy(node->field.text, value);

    node->field.index = FIELD_UNKNOWN;
    node->field.table_id = TABLE_NONE;

    return 0;
}

/**
 * Parses a function
 * Returns -1 for error; flags otherwise
 */
static int parseString(char *value, struct Node *node)
{
    struct Field *field = (struct Field *)node;

    // Detected string literal
    field->index = FIELD_CONSTANT;
    field->table_id = TABLE_NONE;

    int len = strlen(value);

    if (value[len - 1] != '\'')
    {
        fprintf(
            stderr,
            "expected apostrophe got '%c'\n",
            value[len - 1]);
        return -1;
    }

    // We need to strip the leading and trailing single quote and replace
    // escaped characters.
    char *ptr = field->text;

    for (int i = 1; i < len - 1; i++)
    {
        char c = value[i];
        char c2 = value[i + 1];

        // Backslash escapes
        if (c == '\\')
        {
            if (c2 == 't')
            {
                *ptr++ = '\t';
                i++;
            }
            // Will probably cause output issues
            else if (c2 == 'n')
            {
                *ptr++ = '\n';
                i++;
            }
            else if (c2 == '\'')
            {
                *ptr++ = c2;
                i++;
            }
            else
            {
                *ptr++ = c;
            }
        }
        // Double single quote escape
        else if (c == '\'' && c2 == c)
        {
            *ptr++ = c;
            i++;
        }
        // Just a normal character
        else
        {
            *ptr++ = c;
        }
    }

    field->text[len - 2] = '\0';

    return 0;
}

/**
 * Parses a function
 * Returns -1 for error; flags otherwise
 */
static int parseFunction(const char *value, const char *query, size_t *index, struct Node *node, struct Query *q)
{
    if (strcmp(value, "EXTRACT") == 0)
    {
        char part[32];
        getToken(query, index, part, sizeof part);

        if (strcmp(part, "YEAR") == 0)
        {
            node->function = FUNC_EXTRACT_YEAR;
        }
        else if (strcmp(part, "MONTH") == 0)
        {
            node->function = FUNC_EXTRACT_MONTH;
        }
        else if (strcmp(part, "DAY") == 0)
        {
            node->function = FUNC_EXTRACT_DAY;
        }
        else if (strcmp(part, "WEEK") == 0)
        {
            node->function = FUNC_EXTRACT_WEEK;
        }
        else if (strcmp(part, "WEEKYEAR") == 0)
        {
            node->function = FUNC_EXTRACT_WEEKYEAR;
        }
        else if (strcmp(part, "WEEKDAY") == 0)
        {
            node->function = FUNC_EXTRACT_WEEKDAY;
        }
        else if (strcmp(part, "HEYEAR") == 0)
        {
            node->function = FUNC_EXTRACT_HEYEAR;
        }
        else if (strcmp(part, "YEARDAY") == 0)
        {
            node->function = FUNC_EXTRACT_YEARDAY;
        }
        else if (strcmp(part, "MILLENNIUM") == 0)
        {
            node->function = FUNC_EXTRACT_MILLENNIUM;
        }
        else if (strcmp(part, "CENTURY") == 0)
        {
            node->function = FUNC_EXTRACT_CENTURY;
        }
        else if (strcmp(part, "DECADE") == 0)
        {
            node->function = FUNC_EXTRACT_DECADE;
        }
        else if (strcmp(part, "QUARTER") == 0)
        {
            node->function = FUNC_EXTRACT_QUARTER;
        }
        else if (strcmp(part, "DATE") == 0)
        {
            node->function = FUNC_EXTRACT_DATE;
        }
        else if (strcmp(part, "DATETIME") == 0)
        {
            node->function = FUNC_EXTRACT_DATETIME;
        }
        else if (strcmp(part, "JULIAN") == 0)
        {
            node->function = FUNC_EXTRACT_JULIAN;
        }
        else if (strcmp(part, "MONTH_STRING") == 0)
        {
            node->function = FUNC_EXTRACT_MONTH_STRING;
        }
        else if (strcmp(part, "WEEK_STRING") == 0)
        {
            node->function = FUNC_EXTRACT_WEEK_STRING;
        }
        else if (strcmp(part, "YEARDAY_STRING") == 0)
        {
            node->function = FUNC_EXTRACT_YEARDAY_STRING;
        }
        else
        {
            fprintf(stderr, "expected valid extract part - got %s\n", part);
            return -1;
        }

        skipWhitespace(query, index);

        char keyword[10];
        getToken(query, index, keyword, sizeof keyword);

        if (strcmp(keyword, "FROM") != 0)
        {
            fprintf(stderr, "expected FROM\n");
            return -1;
        }

        skipWhitespace(query, index);

        struct Node *child = addChildNode(node);

        parseNode(query, index, child, q);

        skipWhitespace(query, index);

        if (query[*index] != ')')
        {
            fprintf(stderr, "expected ')' got '%c'\n", query[*index]);
            return -1;
        }

        (*index)++;

        return 0;
    }

    if (strcmp(value, "CAST") == 0)
    {
        struct Node *child = addChildNode(node);

        parseNode(query, index, child, q);

        skipWhitespace(query, index);

        char keyword[5];
        getToken(query, index, keyword, sizeof keyword);

        if (strcmp(keyword, "AS") != 0)
        {
            fprintf(stderr, "expected AS\n");
            return -1;
        }

        skipWhitespace(query, index);

        char type[32];
        getToken(query, index, type, sizeof type);

        if (strcmp(type, "INT") == 0 || strcmp(type, "INTERVAL") == 0)
        {
            node->function = FUNC_CAST_INT;
        }
        else if (strcmp(type, "DURATION") == 0)
        {
            node->function = FUNC_CAST_DURATION;
        }
        else
        {
            fprintf(stderr, "cannot cast to %s\n", type);
            return -1;
        }

        if (query[*index] != ')')
        {
            fprintf(stderr, "expected ')' got '%c'\n", query[*index]);
            return -1;
        }

        (*index)++;

        return 0;
    }

    int flags = 0;

    // Regular functions
    if (strcmp(value, "PK") == 0)
    {
        node->function = FUNC_PK;
    }
    else if (strcmp(value, "UNIQUE") == 0)
    {
        node->function = FUNC_UNIQUE;
    }
    else if (strcmp(value, "INDEX") == 0)
    {
        node->function = FUNC_INDEX;
    }
    else if (
        strcmp(value, "CHR") == 0 ||
        strcmp(value, "CHAR") == 0)
    {
        node->function = FUNC_CHR;
    }
    // Get codepoint of first UTF-8 character in value
    else if (strcmp(value, "CODEPOINT") == 0)
    {
        node->function = FUNC_CODEPOINT;
    }
    // Convert mis-encoded string
    else if (strcmp(value, "W1252") == 0)
    {
        node->function = FUNC_W1252;
    }
    else if (strcmp(value, "RANDOM") == 0)
    {
        node->function = FUNC_RANDOM;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "ADD") == 0)
    {
        node->function = FUNC_ADD;
    }
    else if (strcmp(value, "SUB") == 0)
    {
        node->function = FUNC_SUB;
    }
    else if (strcmp(value, "MUL") == 0)
    {
        node->function = FUNC_MUL;
    }
    else if (strcmp(value, "DIV") == 0)
    {
        node->function = FUNC_DIV;
    }
    else if (strcmp(value, "MOD") == 0)
    {
        node->function = FUNC_MOD;
    }
    else if (strcmp(value, "POW") == 0)
    {
        node->function = FUNC_POW;
    }
    else if (strcmp(value, "TO_HEX") == 0)
    {
        node->function = FUNC_TO_HEX;
    }
    else if (strcmp(value, "HEX") == 0)
    {
        node->function = FUNC_HEX;
    }
    else if (strcmp(value, "LENGTH") == 0)
    {
        node->function = FUNC_LENGTH;
    }
    else if (strcmp(value, "LEFT") == 0)
    {
        // LEFT(<field>, <count>)
        node->function = FUNC_LEFT;
    }
    else if (strcmp(value, "RIGHT") == 0)
    {
        // RIGHT(<field>, <count>)
        node->function = FUNC_RIGHT;
    }
    else if (strcmp(value, "DATE_ADD") == 0)
    {
        node->function = FUNC_DATE_ADD;
    }
    else if (strcmp(value, "DATE_SUB") == 0)
    {
        node->function = FUNC_DATE_SUB;
    }
    else if (strcmp(value, "DATE_DIFF") == 0)
    {
        node->function = FUNC_DATE_DIFF;
    }
    else if (strcmp(value, "TODAY") == 0)
    {
        node->function = FUNC_DATE_TODAY;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "NOW") == 0)
    {
        node->function = FUNC_DATE_NOW;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "DATE") == 0)
    {
        node->function = FUNC_DATE_DATE;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "TIME") == 0)
    {
        node->function = FUNC_DATE_TIME;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "CLOCK") == 0)
    {
        node->function = FUNC_DATE_CLOCK;
        node->field.index = FIELD_CONSTANT;
        node->field.table_id = TABLE_NONE;
    }
    else if (strcmp(value, "MAKE_DATE") == 0)
    {
        node->function = FUNC_MAKE_DATE;
    }
    else if (strcmp(value, "MAKE_TIME") == 0)
    {
        node->function = FUNC_MAKE_TIME;
    }
    else if (strcmp(value, "MAKE_DATETIME") == 0)
    {
        node->function = FUNC_MAKE_DATETIME;
    }
    else if (strcmp(value, "INTERVAL") == 0)
    {
        // Synonym for CAST(X AS INT)
        node->function = FUNC_CAST_INT;
    }
    else if (strcmp(value, "COUNT") == 0)
    {
        node->function = FUNC_AGG_COUNT;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(value, "MAX") == 0)
    {
        node->function = FUNC_AGG_MAX;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(value, "MIN") == 0)
    {
        node->function = FUNC_AGG_MIN;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(value, "SUM") == 0)
    {
        node->function = FUNC_AGG_SUM;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(value, "AVG") == 0)
    {
        node->function = FUNC_AGG_AVG;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(value, "LISTAGG") == 0)
    {
        node->function = FUNC_AGG_LISTAGG;
        flags |= FLAG_GROUP;
    }
    else
    {
        fprintf(stderr, "Unknown function: %s\n", value);
        return -1;
    }

    parseFunctionParams(query, index, node, q);

    return flags;
}

#define MAX_PARENS 10

/**
 * @brief Parses simple nodes and maths nodes:
 *
 *  Simple nodes: => <node>
 *      <field>
 *      <constant>
 *      <function>(<node>, ...)
 *  Maths expression nodes: => <node>
 *      <field> + <constant>
 *      <function>() + <constant>
 *      <contant> + <function>()
 *      (<node> + <node>)
 *      etc.
 * @param query {string}
 * @param index {int pointer} pointer to index offset
 * @param node {node pointer} destination node to parse into
 * Returns -1 for error; flags otherwise
 */
int parseNode(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q)
{
    skipWhitespace(query, index);

    int flags = 0;

    if (query[*index] == '(')
    {
        (*index)++;

        // Check to see if it looks like a subquery
        if (strncmp(query + *index, "SELECT", 6) == 0 ||
            strncmp(query + *index, "FROM", 4) == 0)
        {
            int len = find_matching_parenthesis(query + *index - 1);

            if (len >= MAX_TABLE_LENGTH)
            {
                fprintf(
                    stderr,
                    "Subqueries longer than %d are not supported. Subquery was "
                    "%d bytes.\n",
                    MAX_TABLE_LENGTH,
                    len);
                return -1;
            }

            struct Table *table = allocateTable(q);

            strncpy(table->name, query + *index, len - 2);
            table->db = DB_SUBQUERY;

            node->field.index = 0;
            node->field.table_id = q->table_count - 1;
            node->function = FUNC_UNITY;

            (*index) += len - 1;
        }
        else
        {
            node->function = FUNC_PARENS;
            struct Node *root = addChildNode(node);

            flags |= parseNode(query, index, root, q);

            if (flags < 0)
            {
                return flags;
            }

            if (query[*index] != ')')
            {
                fprintf(stderr, "Expecting ')' after expression\n");
                return -1;
            }

            (*index)++;
        }
    }
    else
    {
        // Parse first simple node
        flags |= parseSimpleNode(query, index, node, q);
    }

    while (query[*index] != '\0' && query[*index] != ';')
    {
        enum Function fn = parseSimpleOperators(query, index);

        if (fn == FUNC_UNITY)
        {
            break;
        }

        struct Node *next_child = constructExpressionTree(node, fn);

        skipWhitespace(query, index);

        if (query[*index] == '(')
        {
            (*index)++;
            next_child->function = FUNC_PARENS;
            next_child = addChildNode(next_child);

            flags |= parseNode(query, index, next_child, q);

            if (query[*index] != ')')
            {
                fprintf(stderr, "Expecting ')' after expression\n");
                return -1;
            }

            (*index)++;
        }
        else
        {
            flags |= parseSimpleNode(query, index, next_child, q);
        }
    }

    return flags;
}

/**
 * @brief Helper function to read function params from input stream
 *
 * @param query Whole query string
 * @param index Pointer to index value
 * @param node Pointer to column struct
 * @return int 0 for success; -1 for error
 */
static int parseFunctionParams(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q)
{
    while (query[*index] != '\0' && query[*index] != ')')
    {
        struct Node *child_node = addChildNode(node);

        // getQuotedToken won't get '*' so we'll check manually
        if (query[*index] == '*')
        {
            child_node->child_count = 0;
            child_node->field.text[0] = '*';
            child_node->field.index = FIELD_STAR;
            (*index)++;
        }
        else
        {
            parseNode(query, index, child_node, q);
        }

        skipWhitespace(query, index);

        if (query[*index] != ',')
        {
            break;
        }

        (*index)++;
    }

    if (query[*index] != ')')
    {
        fprintf(stderr, "expected ')' got '%c'\n", query[*index]);
        return -1;
    }

    (*index)++;

    return 0;
}

/**
 * Returns function found; or FUNC_UNITY if nothing found
 */
static enum Function parseSimpleOperators(
    const char *query,
    size_t *index)
{
    enum Function function = FUNC_UNITY;

    skipWhitespace(query, index);

    if (query[*index] == '|' && query[*index + 1] == '|')
    {
        function = FUNC_CONCAT;

        // Will get one more increment below
        (*index)++;
    }
    else
    {
        switch (query[*index])
        {
        case '+':
            function = FUNC_ADD;
            break;
        case '-':
            function = FUNC_SUB;
            break;
        case '*':
            function = FUNC_MUL;
            break;
        case '/':
            function = FUNC_DIV;
            break;
        case '%':
            function = FUNC_MOD;
            break;

        default:
            break;
        }
    }

    if (function != FUNC_UNITY)
    {
        (*index)++;
    }

    return function;
}

static int parseOperator(const char *input)
{
    if (strcmp(input, "=") == 0)
        return OPERATOR_EQ;
    if (strcmp(input, "!=") == 0)
        return OPERATOR_NE;
    if (strcmp(input, "IS") == 0)
        return OPERATOR_EQ;
    if (strcmp(input, "<") == 0)
        return OPERATOR_LT;
    if (strcmp(input, "<=") == 0)
        return OPERATOR_LE;
    if (strcmp(input, ">") == 0)
        return OPERATOR_GT;
    if (strcmp(input, ">=") == 0)
        return OPERATOR_GE;
    if (strcmp(input, "LIKE") == 0)
        return OPERATOR_LIKE;
    if (strcmp(input, "BETWEEN") == 0)
        return OPERATOR_GE;
    if (strcmp(input, "IN") == 0)
        return OPERATOR_EQ;
    return FUNC_UNKNOWN;
}

/**
 * Parses all kind of nodes, including:
 *  Simple nodes: (<node>)
 *      <field>
 *      <constant>
 *  Maths expression nodes: (<node>)
 *      <field> + <constant>
 *      etc.
 *  Nodes with operators of the form:
 *      <node> = <node>
 *      <node> >= <node>
 *      <node> BETWEEN <node> AND <node>
 *      <node IN (<node>, <node>, ...)
 *      etc.
 * return -1 for error
 */
int parseComplexNode(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q)
{
    if (node->child_count != 0)
    {
        fprintf(
            stderr,
            "We need an empty node to parse an operator expression into\n");
        return -1;
    }

    /*******************
     * Parse Left Side
     *******************/

    int flags = parseNode(query, index, node, q);
    if (flags < 0)
    {
        return flags;
    }

    /*******************
     * Parse Operator
     *******************/

    char op[32] = {0};
    int count = getOperatorToken(query, index, op, 31);

    if (count <= 0)
    {
        // No operator, just end now
        return flags;
    }

    enum Function function = parseOperator(op);
    if (function == FUNC_UNKNOWN)
    {
        // No operator
        // Rewind index and bail out
        *index -= count;
        return flags;
    }

    // We do have an operator

    // First clone the node into child
    cloneNodeIntoChild(node);

    // Save this for later if needed
    struct Node *left = &node->children[0];

    // Then reserve space for right hand side
    struct Node *right = addChildNode(node);

    node->function = function;

    // Check for IS NOT
    if (strcmp(op, "IS") == 0)
    {
        skipWhitespace(query, index);
        if (strncmp(query + *index, "NOT ", 4) == 0)
        {
            node->function = OPERATOR_NE;
            *index += 4;
        }
    }

    if (strcmp(op, "IN") == 0)
    {
        skipWhitespace(query, index);

        if (query[*index] != '(')
        {
            fprintf(stderr, "Expected '(' after IN\n");
            return -1;
        }

        (*index)++;

        // Parse first child
        int result = parseNode(query, index, right, q);
        if (result != 0)
        {
            return result;
        }

        /*
            Before:
              =
            /   \
            L   R

            After:
                  OR
               /      \
              =     (   =       )
            /   \   (  / \      )
            L   R1  ( L   R2    )
        */

        cloneNodeIntoChild(node);

        node->function = OPERATOR_OR;

        skipWhitespace(query, index);

        if (query[*index] == ',')
        {
            (*index)++;

            while (query[*index] != '\0' &&
                   query[*index] != ';' &&
                   query[*index] != ')')
            {
                struct Node *next_child = addChildNode(node);

                struct Node *next_left = allocateNodeChildren(next_child, 2);
                struct Node *next_right = next_left + 1;

                next_child->function = OPERATOR_EQ;

                struct Node *clone = &node->children[0];

                copyNodeTree(next_left, &clone->children[0]);

                int result = parseNode(query, index, next_right, q);
                if (result != 0)
                {
                    return result;
                }

                skipWhitespace(query, index);

                if (query[*index] != ',')
                {
                    break;
                }

                (*index)++;
            }
        }

        if (query[*index] != ')')
        {
            fprintf(stderr, "Expected ')' after 'IN ('. Found %c\n", query[*index]);
            return -1;
        }

        (*index)++;

        return 0;
    }

    /*******************
     * Parse Right Side
     *******************/

    int result = parseNode(query, index, right, q);
    if (result != 0)
    {
        return result;
    }

    skipWhitespace(query, index);

    /**************************
     * Parse Second Right Side
     **************************/

    if (strcmp(op, "BETWEEN") == 0)
    {
        enum Function op2 = FUNC_UNKNOWN;

        if (strncmp(query + *index, "AND ", 4) == 0)
        {
            op2 = OPERATOR_LE;
            *index += 4;
        }
        else if (strncmp(query + *index, "ANDX ", 5) == 0)
        {
            op2 = OPERATOR_LT;
            *index += 5;
        }
        else
        {
            fprintf(stderr, "Expected AND or ANDX after BETWEEN\n");
            return -1;
        }

        /*
            Before:
              >=
            /    \
            L     R

            After:
                  AND
               /      \
              >=        <=
            /   \      /  \
            L    R1   L    R2
        */

        cloneNodeIntoChild(node);

        left = &node->children[0].children[0];

        node->function = OPERATOR_AND;

        struct Node *p2 = addChildNode(node);

        struct Node *left2 = allocateNodeChildren(p2, 2);
        struct Node *right2 = left2 + 1;

        p2->function = op2;

        copyNodeTree(left2, left);

        int result = parseNode(query, index, right2, q);
        if (result != 0)
        {
            return result;
        }
    }

    return 0;
}

/**
 * Parses list of complex nodes with AND between
 * Does *not* return a list. Returns single AND node with children as list
 * Parses:
 *      <complex node> AND <complex node> AND ...
 */
int parseNodeList(const char *query, size_t *index, struct Node *node, struct Query *q)
{
    int flags = parseComplexNode(query, index, node, q);
    if (flags < 0)
    {
        return flags;
    }

    while (query[*index] != '\0' && query[*index] != ';')
    {

        skipWhitespace(query, index);

        if (strncmp(&query[*index], "AND ", 4) != 0)
        {
            break;
        }

        *index += 4;

        if (node->function != OPERATOR_AND)
        {
            cloneNodeIntoChild(node);
            node->function = OPERATOR_AND;
        }

        struct Node *next_child = addChildNode(node);

        int result = parseComplexNode(query, index, next_child, q);
        if (result < 0)
        {
            return result;
        }
        flags |= result;
    }

    return flags;
}

/**
 * Should take a partially built node tree and add a new mathematical operator
 * in the correct place as determined by order of operations rules
 *
 * Example Trees:
 *
 * tokens: a + b + c
 * tree:
 *               +               +
 *              / \             / \
 *      a      a   b           a   +
 *                                / \
 *                               b   c
 * tokens: a + b * c
 * tree:
 *               +               +
 *              / \             / \
 *      a      a   b           a   *
 *                                / \
 *                               b   c
 * tokens: a * b - c
 * tree:
 *               *               -
 *              / \             / \
 *      a      a   b           *   c
 *                            / \
 *                           a   b
 * tokens: a + b * c - d
 * tree:
 *               +               +              -
 *              / \             / \            / \
 *      a      a   b           a   *          +   d
 *                                / \        / \
 *                               b   c      a   *
 *                                             / \
 *                                            b   c
 *
 * Lower precedence: New root
 * Higher precedence: Rightmost child
 *
 * Returns node where next node should be parsed into
 */
static struct Node *constructExpressionTree(
    struct Node *node,
    enum Function next_function)
{
    int new_precedence = getPrecedence(next_function);

    struct Node *right;

    // Keep going as far down and right as possible
    struct Node *old_right = node;
    while (old_right->child_count > 0 &&
           new_precedence > getPrecedence(old_right->function))
    {
        old_right = &old_right->children[old_right->child_count - 1];
    }

    cloneNodeIntoChild(old_right);
    old_right->function = next_function;
    right = addChildNode(old_right);

    return right;
}

static int getPrecedence(enum Function fn)
{
    if (fn == FUNC_CONCAT)
        return 5;

    if (fn == FUNC_ADD)
        return 10;
    if (fn == FUNC_SUB)
        return 10;

    if (fn == FUNC_MUL)
        return 20;
    if (fn == FUNC_DIV)
        return 20;
    if (fn == FUNC_MOD)
        return 20;

    if (fn == FUNC_PARENS)
        return 90;

    // All other functions bind their params impossibly tightly
    return 100;
}