#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

#include "../structs.h"
#include "../functions/util.h"
#include "../functions/date.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "./predicates.h"
#include "../db/db.h"

#define IS_NOT_NULL(x) (x[0])

/**
 * @brief
 *
 * @param output
 * @param function
 * @param values
 * @return int number of bytes written; -1 for error
 */
int evaluateFunction(
    char *output,
    int function,
    char **values,
    int value_count)
{

    // Random takes 0 parameters
    if (function == FUNC_RANDOM)
    {
        return sprintf(output, "%d", rand());
    }

    // TODAY takes 0 parameters
    if (function == FUNC_DATE_TODAY)
    {
        struct DateTime dt;
        parseDate("CURRENT_DATE", &dt);
        return sprintf(output, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    // NOW takes 0 parameters
    if (function == FUNC_DATE_NOW)
    {
        struct DateTime dt = {0};
        parseDate("CURRENT_DATE", &dt);
        parseTime("CURRENT_TIME", &dt);
        return sprintf(
            output,
            "%04d-%02d-%02dT%02d:%02d:%02d",
            dt.year, dt.month, dt.day,
            dt.hour, dt.minute, dt.second);
    }

    // CLOCK takes 0 parameters
    if (function == FUNC_DATE_CLOCK)
    {
        struct DateTime dt = {0};
        parseTime("CURRENT_TIME", &dt);
        return sprintf(
            output,
            "%02d:%02d:%02d",
            dt.hour, dt.minute, dt.second);
    }

    // All other functions take at least 1 parameter. If the field is NULL then
    // the whole function evaluates to NULL.
    if (values[0][0] == 0)
    {
        output[0] = '\0';
        return 0;
    }

    if (function == FUNC_UNITY)
    {
        return sprintf(output, "%s", values[0]);
    }
    else if (function == FUNC_CHR)
    {
        int codepoint = atoi(values[0]);
        writeUTF8(values[0], codepoint);

        return sprintf(output, "%s", values[0]);
    }
    // Reads a single codepoint from the first one, two, three, or four bytes.
    else if (function == FUNC_CODEPOINT)
    {
        int codepoint = readUTF8((void *)values[0], NULL);

        return sprintf(output, "%d", codepoint);
    }
    // Somehow will produce an array of code points
    // else if (function == FUNC_VECTOR_CODEPOINT) {
    //     char *end_ptr = values[0];

    //     while (*end_ptr != '\0') {
    //         int codepoint = readUTF8(end_ptr, &end_ptr);
    //         fprintf(stderr, "%d ", codepoint);
    //     }

    //     return sprintf(output, "%d", 0);
    // }
    else if (function == FUNC_HEX)
    {
        uint8_t *ptr = (uint8_t *)values[0];
        int len = 0;
        while (*ptr != '\0')
        {
            len += sprintf(output, "%02X", *ptr);
            output += 2;
            ptr++;
        }
        *output = '\0';
        return len + 1;
    }
    else if (function == FUNC_W1252)
    {
        uint8_t *end_ptr = (void *)values[0];
        int i = 0;

        // Track valid UTF-8 sequences
        int expected_byte_count = 0;
        int byte_start = 0;

        while (*end_ptr != '\0')
        {
            int codepoint = readUTF8(end_ptr, &end_ptr);
            int byte = w1252Map(codepoint);

            if (byte == -1)
            {
                // This means this value wasn't actually w1252 encoded
                // Write the original character back to the output
                i += writeUTF8(output + i, codepoint);
                continue;
            }

            // fprintf(stderr, "0x%02X ", byte);

            int is_continuation = (byte & 0xC0) == 0x80;

            if (expected_byte_count && is_continuation)
            {
                expected_byte_count--;
            }
            else if (expected_byte_count > 0 && !is_continuation)
            {
                // error
                // fprintf(stderr, "Bad UTF-8 sequence: Missing continuation byte\n");
                // rewind
                i = byte_start;
                break;
            }
            else if (expected_byte_count == 0 && is_continuation)
            {
                // error
                // fprintf(stderr, "Bad UTF-8 sequence: Unexpected continuation byte\n");
                // rewind
                i = byte_start;
                break;
            }
            else if (byte < 0x80)
            {
                expected_byte_count = 0;
                byte_start = i;
            }
            else if ((byte & 0xE0) == 0xC0)
            {
                expected_byte_count = 1;
                byte_start = i;
            }
            else if ((byte & 0xF0) == 0xE0)
            {
                expected_byte_count = 2;
                byte_start = i;
            }
            else if ((byte & 0xF8) == 0xF0)
            {
                expected_byte_count = 3;
                byte_start = i;
            }

            output[i] = byte;
            i++;
        }

        if (expected_byte_count > 0)
        {
            // error
            // fprintf(stderr, "Bad UTF-8 sequence: Unexpected end\n");
            // rewind
            i = byte_start;
        }

        output[i] = '\0';

        return i;
    }
    else if (function == FUNC_TO_HEX)
    {
        int val = atoi(values[0]);

        if (val < 0)
        {
            return sprintf(output, "-0x%x", abs(val));
        }
        else if (val < 0x100)
        {
            return sprintf(output, "0x%02x", val);
        }
        else if (val < 0x10000)
        {
            return sprintf(output, "0x%04x", val);
        }
        else
        {
            return sprintf(output, "0x%x", val);
        }
    }
    else if (function == FUNC_ADD)
    {
        // We can add:
        // * int + int
        // * dateTime + int (seconds)
        // * int + dateTime (seconds)
        // * date + int (days)
        // * int + date (days)
        // * time + int (seconds)
        // * int + time (seconds)
        // * time + time

        long val = 0;
        struct DateTime dt = {0};

        enum OperandType result_type = OPERAND_INT;

        for (int i = 0; i < value_count; i++)
        {
            if (parseDateTime(values[i], &dt))
            {
                if (result_type == OPERAND_INT)
                {
                    result_type = OPERAND_DATE_TIME;
                    val += datetimeGetUnix(&dt);
                }
                else
                {
                    fprintf(stderr, "Unexpected DateTime: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else if (parseDate(values[i], &dt))
            {
                if (result_type == OPERAND_INT)
                {
                    result_type = OPERAND_DATE;
                    val += datetimeGetJulian(&dt);
                }
                else
                {
                    fprintf(stderr, "Unexpected Date: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else if (parseTime(values[i], &dt))
            {
                if (result_type == OPERAND_INT || result_type == OPERAND_TIME)
                {
                    result_type = OPERAND_TIME;
                    val += timeInSeconds(&dt);
                }
                else
                {
                    fprintf(stderr, "Unexpected Time: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else
            {
                val += atol(values[i]);
            }
        }

        switch (result_type)
        {
        case OPERAND_DATE_TIME:
            datetimeFromUnix(&dt, val);
            return sprintDateTime(output, &dt);
        case OPERAND_DATE:
            datetimeFromJulian(&dt, val);
            return sprintDate(output, &dt);
        case OPERAND_TIME:
            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        default:
            return sprintf(output, "%ld", val);
        }
    }
    else if (function == FUNC_SUB)
    {
        // We can subtract:
        // * int - int
        // * dateTime - int (seconds)
        // * dateTime - dateTime
        // * date - int     (days)
        // * date - date    (days)
        // * time - int     (seconds)
        // * time - time    (seconds)

        long val = 0;
        struct DateTime dt = {0};

        enum OperandType result_type = OPERAND_UNKNOWN;

        for (int i = 0; i < value_count; i++)
        {

            if (parseDateTime(values[i], &dt))
            {
                if (result_type == OPERAND_UNKNOWN)
                {
                    val = datetimeGetUnix(&dt);
                    result_type = OPERAND_DATE_TIME;
                }
                else if (result_type == OPERAND_DATE_TIME)
                {
                    val -= datetimeGetUnix(&dt);
                    result_type = OPERAND_INT;
                }
                else
                {
                    fprintf(stderr, "Unexpected DateTime: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else if (parseDate(values[i], &dt))
            {
                if (result_type == OPERAND_UNKNOWN)
                {
                    val = datetimeGetJulian(&dt);
                    result_type = OPERAND_DATE;
                }
                else if (result_type == OPERAND_DATE)
                {
                    val -= datetimeGetJulian(&dt);
                    result_type = OPERAND_INT;
                }
                else
                {
                    fprintf(stderr, "Unexpected Date: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else if (parseTime(values[i], &dt))
            {
                if (result_type == OPERAND_UNKNOWN)
                {
                    val = timeInSeconds(&dt);
                    result_type = OPERAND_TIME;
                }
                else if (result_type == OPERAND_TIME)
                {
                    val -= timeInSeconds(&dt);
                    result_type = OPERAND_INT;
                }
                else
                {
                    fprintf(stderr, "Unexpected Time: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else
            {
                if (result_type == OPERAND_UNKNOWN)
                {
                    val = atol(values[i]);
                    result_type = OPERAND_INT;
                }
                else
                {
                    val -= atol(values[i]);
                }
            }
        }

        switch (result_type)
        {
        case OPERAND_DATE_TIME:
            datetimeFromUnix(&dt, val);
            return sprintDateTime(output, &dt);
        case OPERAND_DATE:
            datetimeFromJulian(&dt, val);
            return sprintDate(output, &dt);
        case OPERAND_TIME:
            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        default:
            return sprintf(output, "%ld", val);
        }
    }
    else if (function == FUNC_MUL)
    {
        // We can do:
        //  int * int
        //  time * int

        long val = 1;
        struct DateTime dt = {0};
        int is_time = 0;

        for (int i = 0; i < value_count; i++)
        {
            if (parseTime(values[i], &dt))
            {
                if (i == 0)
                {
                    val = timeInSeconds(&dt);
                    is_time = 1;
                }
                else
                {
                    fprintf(stderr, "Unexpected Time: %s\n", values[i]);
                    output[0] = 0;
                    return 0;
                }
            }
            else
            {
                val *= atol(values[i]);
            }
        }

        if (is_time)
        {
            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        }

        return sprintf(output, "%ld", val);
    }
    else if (function == FUNC_DIV)
    {
        // We can do:
        //  int / int
        //  time / int

        long val = atol(values[0]);
        struct DateTime dt = {0};
        int is_time = 0;

        if (parseTime(values[0], &dt))
        {
            val = timeInSeconds(&dt);
            is_time = 1;
        }

        for (int i = 1; i < value_count; i++)
        {
            long val2 = atol(values[i]);

            if (val2 == 0)
            {
                output[0] = '\0';
                return 0;
            }

            val /= val2;
        }

        if (is_time)
        {
            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        }

        return sprintf(output, "%ld", val);
    }
    else if (function == FUNC_MOD)
    {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        if (val2 == 0)
        {
            output[0] = '\0';
            return 0;
        }

        return sprintf(output, "%ld", val1 % val2);
    }
    else if (function == FUNC_POW)
    {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        if (val2 < 0)
        {
            return sprintf(output, "0");
        }

        long prod = 1;

        for (int i = 0; i < val2; i++)
        {
            prod *= val1;
        }

        return sprintf(output, "%ld", prod);
    }
    else if (function == FUNC_PARENS)
    {
        return sprintf(output, "%s", values[0]);
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_STRING)
    {
        if (function == FUNC_LENGTH)
        {
            int len = strlen(values[0]);
            return sprintf(output, "%d", len);
        }
        else if (function == FUNC_LEFT)
        {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count)
            {
                strncpy(output, values[0], count);
                output[count] = '\0';
                return count;
            }
            else
            {
                return sprintf(output, "%s", values[0]);
            }
        }
        else if (function == FUNC_RIGHT)
        {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count)
            {
                return sprintf(output, "%s", values[0] + len - count);
            }
            else
            {
                return sprintf(output, "%s", values[0]);
            }
        }
        else if (function == FUNC_CONCAT)
        {
            int count = 0;

            for (int i = 0; i < value_count; i++)
            {
                int size = sprintf(output, "%s", values[i]);
                count += size;
                output += size;
            }

            return count;
        }
        else if (function == FUNC_CAST_INT)
        {
            // Casts:
            //   numbers to integers
            //   duration to integer ('00:01:30' to 900)

            struct DateTime dt = {0};
            if (parseTime(values[0], &dt))
            {
                int result = timeInSeconds(&dt);

                return sprintf(output, "%d", result);
            }

            long result = strtol(values[0], NULL, 10);

            return sprintf(output, "%ld", result);
        }
        else if (function == FUNC_CAST_DURATION)
        {
            // Casts:
            //   numbers to durations (900 to '00:15:00')

            struct DateTime dt = {0};

            if (parseTime(values[0], &dt))
            {
                // Already a duration
                return sprintf(output, "%s", values[0]);
            }

            long result = strtol(values[0], NULL, 10);

            timeFromSeconds(&dt, result);

            return sprintTime(output, &dt);
        }
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_EXTRACT)
    {
        struct DateTime dt;

        if (!parseDate(values[0], &dt))
        {
            return 0;
        }

        if (function == FUNC_EXTRACT_YEAR)
        {
            return sprintf(output, "%d", dt.year);
        }
        else if (function == FUNC_EXTRACT_MONTH)
        {
            return sprintf(output, "%d", dt.month);
        }
        else if (function == FUNC_EXTRACT_DAY)
        {
            return sprintf(output, "%d", dt.day);
        }
        else if (function == FUNC_EXTRACT_WEEK)
        {
            return sprintf(output, "%d", datetimeGetWeek(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKYEAR)
        {
            return sprintf(output, "%d", datetimeGetWeekYear(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKDAY)
        {
            return sprintf(output, "%d", datetimeGetWeekDay(&dt));
        }
        else if (function == FUNC_EXTRACT_HEYEAR)
        {
            return sprintf(output, "%d", dt.year + 10000);
        }
        else if (function == FUNC_EXTRACT_YEARDAY)
        {
            return sprintf(output, "%d", datetimeGetYearDay(&dt));
        }
        else if (function == FUNC_EXTRACT_MILLENNIUM)
        {
            return sprintf(output, "%d", dt.year / 1000);
        }
        else if (function == FUNC_EXTRACT_CENTURY)
        {
            return sprintf(output, "%d", dt.year / 100);
        }
        else if (function == FUNC_EXTRACT_DECADE)
        {
            return sprintf(output, "%d", dt.year / 10);
        }
        else if (function == FUNC_EXTRACT_QUARTER)
        {
            return sprintf(output, "%d", (dt.month - 1) / 3 + 1);
        }
        else if (function == FUNC_EXTRACT_JULIAN)
        {
            return sprintf(output, "%d", datetimeGetJulian(&dt));
        }
        else if (function == FUNC_EXTRACT_DATE)
        {
            return sprintDate(output, &dt);
        }
        else if (function == FUNC_EXTRACT_DATETIME)
        {
            return sprintf(
                output,
                "%04d-%02d-%02dT%02d:%02d:%02d",
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second);
        }
        else if (function == FUNC_EXTRACT_MONTH_STRING)
        {
            return sprintf(output, "%04d-%02d", dt.year, dt.month);
        }
        else if (function == FUNC_EXTRACT_WEEK_STRING)
        {
            return sprintf(
                output,
                "%04d-W%02d",
                datetimeGetWeekYear(&dt),
                datetimeGetWeek(&dt));
        }
        else if (function == FUNC_EXTRACT_YEARDAY_STRING)
        {
            return sprintf(output, "%04d-%03d", dt.year, datetimeGetYearDay(&dt));
        }
        else
        {
            return sprintf(output, "BADEXTRACT");
        }
    }
    else if (function == FUNC_DATE_ADD)
    {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDate(values[0], &dt1))
        {
            return 0;
        }

        int diff = atoi(values[1]);

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = julian1 + diff;

        datetimeFromJulian(&dt2, julian2);

        return sprintDate(output, &dt2);
    }
    else if (function == FUNC_DATE_SUB)
    {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDate(values[0], &dt1))
        {
            return 0;
        }

        int diff = atoi(values[1]);

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = julian1 - diff;

        datetimeFromJulian(&dt2, julian2);

        return sprintDate(output, &dt2);
    }
    else if (function == FUNC_DATE_DIFF)
    {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDate(values[0], &dt1))
        {
            return 0;
        }

        if (!parseDate(values[1], &dt2))
        {
            return 0;
        }

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = datetimeGetJulian(&dt2);

        return sprintf(output, "%d", julian1 - julian2);
    }
    else if (function == FUNC_DATE_DATE)
    {
        struct DateTime dt;
        if (!parseDateTime(values[0], &dt))
        {
            return 0;
        }

        return sprintDate(output, &dt);
    }
    else if (function == FUNC_DATE_TIME)
    {
        struct DateTime dt;
        if (!parseDateTime(values[0], &dt))
        {
            return 0;
        }

        return sprintf(
            output,
            "%02d:%02d:%02d",
            dt.hour, dt.minute, dt.second);
    }
    else if (function == FUNC_MAKE_DATE)
    {
        struct DateTime dt;
        // Default to 0
        dt.year = 0;
        // Default to 1
        dt.month = 1;
        // Default to 1
        dt.day = 1;

        if (value_count > 0)
        {
            dt.year = atoi(values[0]);

            if (value_count > 1)
            {
                dt.month = atoi(values[1]);

                if (value_count > 2)
                {
                    dt.day = atoi(values[2]);
                }
            }
        }

        return sprintDate(output, &dt);
    }
    else if (function == FUNC_MAKE_TIME)
    {
        int h = 0;
        int m = 0;
        int s = 0;

        // Default to 0
        if (value_count > 0)
        {
            h = atoi(values[0]);

            if (value_count > 1)
            {
                m = atoi(values[1]);

                if (value_count > 2)
                {
                    s = atoi(values[2]);
                }
            }
        }

        return sprintf(output, "%02d:%02d:%02d", h, m, s);
    }
    else if (function == FUNC_MAKE_DATETIME)
    {
        int y = 0;
        int m = 0;
        int d = 0;
        int h = 0;
        int i = 0;
        int s = 0;

        // Default to 0
        if (value_count > 0)
        {
            y = atoi(values[0]);

            if (value_count > 1)
            {
                m = atoi(values[1]);

                if (value_count > 2)
                {
                    d = atoi(values[2]);

                    if (value_count > 3)
                    {
                        h = atoi(values[3]);

                        if (value_count > 4)
                        {
                            i = atoi(values[4]);

                            if (value_count > 5)
                            {
                                s = atoi(values[5]);
                            }
                        }
                    }
                }
            }
        }

        return sprintf(
            output,
            "%04d-%02d-%02dT%02d:%02d:%02d",
            y, m, d, h, i, s);
    }

    fprintf(stderr, "Unable to evaluate function %d\n", function);
    return -1;
}

typedef char LongestValue[MAX_VALUE_LENGTH];

/**
 * @brief
 *
 * @param output
 * @param tables
 * @param node
 * @param row_list
 * @return int bytes written; -1 for error
 */
int evaluateAggregateFunction(
    char *output,
    struct Table *tables,
    struct Node *node,
    RowListIndex list_id)
{
    if ((node->function & MASK_FUNC_FAMILY) != FUNC_FAM_AGG)
    {
        return -1;
    }

    if (node->child_count > 1)
    {
        fprintf(stderr, "Not supported: Multi-param agg functions\n");
        exit(-1);
    }

    int is_count_star = node->function == FUNC_AGG_COUNT && ((node->child_count == -1 && node->field.index == FIELD_STAR) ||
                                                             (node->child_count == 1 && node->children[0].field.index == FIELD_STAR));

    struct RowList *row_list = getRowList(list_id);

    LongestValue *values = NULL;

    // We only need to allocate if it's an agg function other than COUNT(*)
    if (!is_count_star)
    {
        values = malloc(row_list->row_count * MAX_VALUE_LENGTH);

        if (values == NULL)
        {
            fprintf(stderr, "Unable to allocate %d bytes\n", row_list->row_count * MAX_VALUE_LENGTH);
            exit(-1);
        }
    }

    int row_count = 0;

    // COUNT(*) without FILTER doesn't need extra processing
    if (is_count_star && node->filter == NULL)
    {
        row_count = row_list->row_count;
    }
    else
        for (unsigned int i = 0; i < row_list->row_count; i++)
        {
            int include = 1;

            if (node->filter != NULL)
            {
                include = evaluateOperatorNode(tables, list_id, i, node->filter);
            }

            if (include)
            {

                // We don't need to evaluate nodes if it's COUNT(*)
                if (!is_count_star)
                {
                    if (node->child_count == -1)
                    {
                        // Self-child optimisation
                        // Must be a single field
                        struct Field *field = (struct Field *)node;

                        int rowid = getRowID(row_list, field->table_id, i);

                        getRecordValue(
                            tables[field->table_id].db,
                            rowid,
                            field->index,
                            values[i],
                            MAX_VALUE_LENGTH);
                    }
                    else if (node->child_count == 1)
                    {
                        // Evaluate (only) child node for each item in the RowList
                        evaluateNode(
                            tables,
                            list_id,
                            i,
                            &node->children[0],
                            values[i],
                            MAX_VALUE_LENGTH);
                    }
                }

                row_count++;
            }
        }

    int bytesWritten = 0;

    if (node->function == FUNC_AGG_COUNT)
    {
        if (is_count_star)
        {
            bytesWritten = sprintf(output, "%d", row_count);
        }
        else
        {
            int count = 0;

            for (int i = 0; i < row_count; i++)
            {
                // Count up the non-NULL values
                if (IS_NOT_NULL(values[i]))
                {
                    count++;
                }
            }

            bytesWritten = sprintf(output, "%d", count);
        }
    }
    else if (node->function == FUNC_AGG_MIN)
    {
        int min = INT_MAX;

        for (int i = 0; i < row_count; i++)
        {
            // Only consider the non-NULL values
            if (IS_NOT_NULL(values[0]))
            {
                int v = atoi(values[i]);

                if (v < min)
                    min = v;
            }
        }

        if (min < INT_MAX)
        {
            bytesWritten = sprintf(output, "%d", min);
        }
    }
    else if (node->function == FUNC_AGG_MAX)
    {
        int max = INT_MIN;

        for (int i = 0; i < row_count; i++)
        {
            // Only consider the non-NULL values
            if (IS_NOT_NULL(values[i]))
            {
                int v = atoi(values[i]);

                if (v > max)
                    max = v;
            }
        }

        if (max > INT_MIN)
        {
            bytesWritten = sprintf(output, "%d", max);
        }
    }
    else if (node->function == FUNC_AGG_SUM)
    {
        int sum = 0;
        int non_null = 0;

        for (int i = 0; i < row_count; i++)
        {
            // Sum the non-NULL values
            if (IS_NOT_NULL(values[i]))
            {
                non_null = 1;
                sum += atoi(values[i]);
            }
        }

        // If *all* rows are NULL then the result is NULL
        if (non_null)
        {
            bytesWritten = sprintf(output, "%d", sum);
        }
    }
    else if (node->function == FUNC_AGG_AVG)
    {
        int count = 0;
        int sum = 0;

        for (int i = 0; i < row_count; i++)
        {
            // Count up the non-NULL values
            if (IS_NOT_NULL(values[i]))
            {
                count++;

                sum += atoi(values[i]);
            }
        }

        if (count > 0)
        {
            bytesWritten = sprintf(output, "%d", sum / count);
        }
    }
    else if (node->function == FUNC_AGG_LISTAGG)
    {
        int have_prev = 0;

        char *output_start = output;

        for (int i = 0; i < row_count; i++)
        {
            // Count up the non-NULL values
            if (IS_NOT_NULL(values[i]))
            {
                if (have_prev == 1)
                {
                    sprintf(output++, ",");
                }

                output += sprintf(output, "%s", values[i]);

                bytesWritten = output - output_start;

                if (bytesWritten > MAX_VALUE_LENGTH)
                {
                    // Overflow! Just bail
                    return bytesWritten;
                }

                have_prev = 1;
            }
        }
    }
    else
    {
        bytesWritten = -1;
    }

    free(values);

    if (bytesWritten == 0)
    {
        output[0] = '\0';
    }

    return bytesWritten;
}
