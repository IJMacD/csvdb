#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>
#include <complex.h>
#include <float.h>
#include <time.h>
#include <ctype.h>

#include "../structs.h"
#include "../gitversion.h"
#include "../functions/util.h"
#include "../functions/date.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "./predicates.h"
#include "../db/db.h"

static int values_have_complex(char **values, int value_count)
{
    for (int i = 0; i < value_count; i++)
    {
        if (is_complex(values[i]))
            return 1;
    }
    return 0;
}

static int parse_complex_value(const char *value, double complex *out)
{
    double re, im;
    if (!parse_complex(value, &re, &im))
        return 0;
    *out = re + im * I;
    return 1;
}

/**
 * Parse DATE_ADD/DATE_SUB amount: either a plain integer (days), an
 * "N UNIT" interval string, or (amount, unit) via values[1]/values[2].
 * Returns 0 on success.
 */
static int parseDateAddAmount(
    char **values,
    int value_count,
    int *count,
    enum DateUnit *unit)
{
    *unit = DATE_UNIT_DAY;
    *count = 0;

    if (value_count >= 3)
    {
        *count = atoi(values[1]);
        *unit = parseDateUnit(values[2]);
        if (*unit == DATE_UNIT_INVALID)
        {
            return -1;
        }
        return 0;
    }

    if (value_count < 2)
    {
        return -1;
    }

    // Plain integer → days (historic DATE_ADD(date, n))
    const char *p = values[1];
    char *end = NULL;
    long amount = strtol(p, &end, 10);
    if (end != p && *end == '\0')
    {
        *count = (int)amount;
        *unit = DATE_UNIT_DAY;
        return 0;
    }

    // "N UNIT" (from INTERVAL n UNIT calendar forms, or path B)
    while (*p == ' ')
        p++;
    amount = strtol(p, &end, 10);
    if (end == p)
    {
        return -1;
    }
    while (*end == ' ')
        end++;
    *unit = parseDateUnit(end);
    if (*unit == DATE_UNIT_INVALID)
    {
        return -1;
    }
    *count = (int)amount;
    return 0;
}

static int evaluateDateAddSub(
    char *output,
    char **values,
    int value_count,
    int sign)
{
    struct DateTime dt = {0};
    int is_datetime = 0;

    if (parseDateTime(values[0], &dt))
    {
        is_datetime = 1;
    }
    else if (!parseDate(values[0], &dt))
    {
        return 0;
    }

    int count = 0;
    enum DateUnit unit = DATE_UNIT_DAY;
    if (parseDateAddAmount(values, value_count, &count, &unit) != 0)
    {
        return 0;
    }

    dateAddUnit(&dt, sign * count, unit);

    // Time units or datetime inputs → datetime string; else date
    if (is_datetime || unit == DATE_UNIT_HOUR || unit == DATE_UNIT_MINUTE ||
        unit == DATE_UNIT_SECOND)
    {
        if (!is_datetime)
        {
            dt.hour = 0;
            dt.minute = 0;
            dt.second = 0;
            if (dt.tz_offset_minutes == TZ_LOCAL)
            {
                /* leave local */
            }
        }
        return sprintDateTime(output, &dt);
    }

    return sprintDate(output, &dt);
}

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

    // VERSION takes 0 parameters
    if (function == FUNC_VERSION)
    {
        return sprintf(output, "%s", gitversion);
    }

    // PI takes 0 parameters
    if (function == FUNC_PI)
    {
        return sprint_number(output, acos(-1.0));
    }

    // TODAY takes 0 parameters
    if (function == FUNC_DATE_TODAY)
    {
        struct DateTime dt;
        parseDate("CURRENT_DATE", &dt);
        return sprintf(output, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    // NOW() / NOW(UTC) / NOW(LOCAL) — optional bare keyword (or quoted synonym)
    if (function == FUNC_DATE_NOW)
    {
        struct DateTime dt = {0};
        time_t t = time(NULL);
        short off = 0;
        int want_local = 0;

        if (value_count >= 1 && values[0] != NULL && values[0][0] != '\0')
        {
            if (strcmp(values[0], "LOCAL") == 0)
            {
                want_local = 1;
            }
            else if (
                strcmp(values[0], "UTC") == 0 ||
                strcmp(values[0], "Z") == 0)
            {
                want_local = 0;
            }
            else
            {
                fprintf(
                    stderr,
                    "NOW() expects LOCAL, UTC, or Z; got %s\n",
                    values[0]);
                return 0;
            }
        }

        if (!getLocalTzOffsetMinutes(&off))
        {
            // Offset unknown: always emit untagged local wall time
            dt.tz_offset_minutes = TZ_LOCAL;
            dt.tz_format = TZ_FMT_Z;
        }
        else if (want_local)
        {
            dt.tz_offset_minutes = off;
            dt.tz_format = TZ_FMT_OFFSET; // LOCAL at UTC still shows +00:00
        }
        else
        {
            dt.tz_offset_minutes = 0;
            dt.tz_format = TZ_FMT_Z;
        }
        datetimeFromUnix(&dt, t);
        return sprintDateTime(output, &dt);
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

    // CHAR/CHR accepts multiple code points and skips NULLs, so handle it
    // before the generic "first arg NULL → NULL" rule.
    if (function == FUNC_CHR)
    {
        char *out = output;
        int written = 0;
        int any_non_null = 0;

        for (int i = 0; i < value_count; i++)
        {
            if (values[i] == NULL || values[i][0] == '\0')
            {
                continue;
            }

            any_non_null = 1;

            int codepoint = atoi(values[i]);
            char tmp[5];
            int n = writeUTF8(tmp, codepoint);
            if (n <= 0)
            {
                continue;
            }

            if (written + n >= MAX_VALUE_LENGTH)
            {
                break;
            }

            memcpy(out, tmp, (size_t)n);
            out += n;
            written += n;
        }

        *out = '\0';

        // All arguments NULL → NULL; otherwise the concatenated characters.
        if (!any_non_null)
        {
            return 0;
        }

        return written;
    }

    // Most functions take at least 1 parameter. If the first field is NULL then
    // the whole function evaluates to NULL. Zero-arg builders (MAKE_DATE /
    // MAKE_TIME / MAKE_DATETIME) are allowed through with defaults.
    if (value_count > 0 && values[0][0] == 0)
    {
        output[0] = '\0';
        return 0;
    }

    if (function == FUNC_UNITY)
    {
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
        // * number + number
        // * complex + complex (promotes reals)
        // * dateTime + int (seconds)
        // * int + dateTime (seconds)
        // * date + int (days)
        // * int + date (days)
        // * time + int (seconds)
        // * int + time (seconds)
        // * time + time

        if (values_have_complex(values, value_count))
        {
            double complex sum = 0;
            for (int i = 0; i < value_count; i++)
            {
                double complex z;
                if (!parse_complex_value(values[i], &z))
                {
                    output[0] = '\0';
                    return 0;
                }
                sum += z;
            }
            return sprint_complex(output, creal(sum), cimag(sum));
        }

        long val = 0;
        double dval = 0;
        struct DateTime dt = {0};

        enum OperandType result_type = OPERAND_INT;

        for (int i = 0; i < value_count; i++)
        {
            if (parseDateTime(values[i], &dt))
            {
                if (result_type == OPERAND_INT)
                {
                    result_type = OPERAND_DATE_TIME;
                    val = (long)dval + datetimeGetUnix(&dt);
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
                    val = (long)dval + datetimeGetJulian(&dt);
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
                    if (result_type == OPERAND_INT)
                    {
                        val = (long)dval;
                    }
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
            else if (result_type == OPERAND_INT)
            {
                dval += strtod(values[i], NULL);
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
            return sprint_number(output, dval);
        }
    }
    else if (function == FUNC_SUB)
    {
        // We can subtract:
        // * number - number
        // * complex - complex (promotes reals)
        // * dateTime - int (seconds)
        // * dateTime - dateTime
        // * date - int     (days)
        // * date - date    (days)
        // * time - int     (seconds)
        // * time - time    (seconds)

        if (values_have_complex(values, value_count))
        {
            double complex result = 0;
            for (int i = 0; i < value_count; i++)
            {
                double complex z;
                if (!parse_complex_value(values[i], &z))
                {
                    output[0] = '\0';
                    return 0;
                }
                if (i == 0)
                    result = z;
                else
                    result -= z;
            }
            return sprint_complex(output, creal(result), cimag(result));
        }

        long val = 0;
        double dval = 0;
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
                    dval = (double)val;
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
                    dval = (double)val;
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
                    dval = (double)val;
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
                    dval = strtod(values[i], NULL);
                    result_type = OPERAND_INT;
                }
                else if (result_type == OPERAND_INT)
                {
                    dval -= strtod(values[i], NULL);
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
            return sprint_number(output, dval);
        }
    }
    else if (function == FUNC_MUL)
    {
        // We can do:
        //  number * number
        //  complex * complex (promotes reals)
        //  time * int

        if (values_have_complex(values, value_count))
        {
            double complex product = 1;
            for (int i = 0; i < value_count; i++)
            {
                double complex z;
                if (!parse_complex_value(values[i], &z))
                {
                    output[0] = '\0';
                    return 0;
                }
                product *= z;
            }
            return sprint_complex(output, creal(product), cimag(product));
        }

        long val = 1;
        double dval = 1;
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
            else if (is_time)
            {
                val *= atol(values[i]);
            }
            else
            {
                dval *= strtod(values[i], NULL);
            }
        }

        if (is_time)
        {
            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        }

        return sprint_number(output, dval);
    }
    else if (function == FUNC_DIV)
    {
        // We can do:
        //  number / number
        //  complex / complex (promotes reals)
        //  time / int

        if (values_have_complex(values, value_count))
        {
            double complex result;
            if (!parse_complex_value(values[0], &result))
            {
                output[0] = '\0';
                return 0;
            }

            for (int i = 1; i < value_count; i++)
            {
                double complex z;
                if (!parse_complex_value(values[i], &z))
                {
                    output[0] = '\0';
                    return 0;
                }
                if (creal(z) == 0 && cimag(z) == 0)
                {
                    output[0] = '\0';
                    return 0;
                }
                result /= z;
            }
            return sprint_complex(output, creal(result), cimag(result));
        }

        struct DateTime dt = {0};

        if (parseTime(values[0], &dt))
        {
            long val = timeInSeconds(&dt);

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

            timeFromSeconds(&dt, val);
            return sprintTime(output, &dt);
        }

        double dval = strtod(values[0], NULL);

        for (int i = 1; i < value_count; i++)
        {
            double val2 = strtod(values[i], NULL);

            if (val2 == 0)
            {
                output[0] = '\0';
                return 0;
            }

            dval /= val2;
        }

        return sprint_number(output, dval);
    }
    else if (function == FUNC_MOD)
    {
        // Real-only
        if (is_complex(values[0]) || (value_count >= 2 && is_complex(values[1])))
        {
            output[0] = '\0';
            return 0;
        }

        double val1 = strtod(values[0], NULL);
        double val2 = strtod(values[1], NULL);

        if (val2 == 0)
        {
            output[0] = '\0';
            return 0;
        }

        return sprint_number(output, fmod(val1, val2));
    }
    else if (function == FUNC_POW)
    {
        if (values_have_complex(values, value_count))
        {
            double complex base, expn;
            if (!parse_complex_value(values[0], &base) ||
                !parse_complex_value(values[1], &expn))
            {
                output[0] = '\0';
                return 0;
            }
            double complex result = cpow(base, expn);
            return sprint_complex(output, creal(result), cimag(result));
        }

        double val1 = strtod(values[0], NULL);
        double val2 = strtod(values[1], NULL);

        return sprint_number(output, pow(val1, val2));
    }
    else if (function == FUNC_ABS)
    {
        if (is_complex(values[0]))
        {
            double complex z;
            if (!parse_complex_value(values[0], &z))
            {
                output[0] = '\0';
                return 0;
            }
            return sprint_number(output, cabs(z));
        }
        return sprint_number(output, fabs(strtod(values[0], NULL)));
    }
    else if (function == FUNC_SQRT)
    {
        if (is_complex(values[0]))
        {
            double complex z;
            if (!parse_complex_value(values[0], &z))
            {
                output[0] = '\0';
                return 0;
            }
            double complex result = csqrt(z);
            return sprint_complex(output, creal(result), cimag(result));
        }

        double val = strtod(values[0], NULL);

        // Negative reals use the principal complex square root
        if (val < 0)
        {
            double complex result = csqrt(val + 0 * I);
            return sprint_complex(output, creal(result), cimag(result));
        }

        return sprint_number(output, sqrt(val));
    }
    else if (function == FUNC_FLOOR)
    {
        if (is_complex(values[0]))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_number(output, floor(strtod(values[0], NULL)));
    }
    else if (function == FUNC_CEIL)
    {
        if (is_complex(values[0]))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_number(output, ceil(strtod(values[0], NULL)));
    }
    else if (function == FUNC_ROUND)
    {
        if (is_complex(values[0]) ||
            (value_count >= 2 && is_complex(values[1])))
        {
            output[0] = '\0';
            return 0;
        }

        double val = strtod(values[0], NULL);

        if (value_count >= 2)
        {
            double digits = strtod(values[1], NULL);
            double scale = pow(10.0, digits);
            return sprint_number(output, round(val * scale) / scale);
        }

        return sprint_number(output, round(val));
    }
    else if (function == FUNC_EXP)
    {
        if (is_complex(values[0]))
        {
            double complex z;
            if (!parse_complex_value(values[0], &z))
            {
                output[0] = '\0';
                return 0;
            }
            double complex result = cexp(z);
            return sprint_complex(output, creal(result), cimag(result));
        }
        return sprint_number(output, exp(strtod(values[0], NULL)));
    }
    else if (function == FUNC_LN)
    {
        if (is_complex(values[0]))
        {
            double complex z;
            if (!parse_complex_value(values[0], &z))
            {
                output[0] = '\0';
                return 0;
            }
            if (creal(z) == 0 && cimag(z) == 0)
            {
                output[0] = '\0';
                return 0;
            }
            double complex result = clog(z);
            return sprint_complex(output, creal(result), cimag(result));
        }

        double val = strtod(values[0], NULL);

        if (val < 0)
        {
            double complex result = clog(val + 0 * I);
            return sprint_complex(output, creal(result), cimag(result));
        }

        if (val <= 0)
        {
            output[0] = '\0';
            return 0;
        }

        return sprint_number(output, log(val));
    }
    else if (function == FUNC_LOG10)
    {
        if (is_complex(values[0]))
        {
            double complex z;
            if (!parse_complex_value(values[0], &z))
            {
                output[0] = '\0';
                return 0;
            }
            if (creal(z) == 0 && cimag(z) == 0)
            {
                output[0] = '\0';
                return 0;
            }
            // clog10 is not universally available; use clog / log(10)
            double complex result = clog(z) / log(10.0);
            return sprint_complex(output, creal(result), cimag(result));
        }

        double val = strtod(values[0], NULL);

        if (val < 0)
        {
            double complex result = clog(val + 0 * I) / log(10.0);
            return sprint_complex(output, creal(result), cimag(result));
        }

        if (val <= 0)
        {
            output[0] = '\0';
            return 0;
        }

        return sprint_number(output, log10(val));
    }
    else if (function == FUNC_COMPLEX)
    {
        if (value_count < 2 || is_complex(values[0]) || is_complex(values[1]))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_complex(
            output,
            strtod(values[0], NULL),
            strtod(values[1], NULL));
    }
    else if (function == FUNC_RE)
    {
        double re, im;
        if (!parse_complex(values[0], &re, &im))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_number(output, re);
    }
    else if (function == FUNC_IM)
    {
        double re, im;
        if (!parse_complex(values[0], &re, &im))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_number(output, im);
    }
    else if (function == FUNC_CONJ)
    {
        double re, im;
        if (!parse_complex(values[0], &re, &im))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_complex(output, re, -im);
    }
    else if (function == FUNC_ARG)
    {
        double re, im;
        if (!parse_complex(values[0], &re, &im))
        {
            output[0] = '\0';
            return 0;
        }
        return sprint_number(output, atan2(im, re));
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
        struct DateTime dt = {0};

        if (!parseDateOrDateTime(values[0], &dt))
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
        else if (function == FUNC_EXTRACT_HOUR)
        {
            return sprintf(output, "%d", dt.hour);
        }
        else if (function == FUNC_EXTRACT_MINUTE)
        {
            return sprintf(output, "%d", dt.minute);
        }
        else if (function == FUNC_EXTRACT_SECOND)
        {
            return sprintf(output, "%d", dt.second);
        }
        else if (function == FUNC_EXTRACT_TIME)
        {
            return sprintTime(output, &dt);
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
        return evaluateDateAddSub(output, values, value_count, 1);
    }
    else if (function == FUNC_DATE_SUB)
    {
        return evaluateDateAddSub(output, values, value_count, -1);
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
    else if (function == FUNC_DATE_TRUNC)
    {
        if (value_count < 2)
        {
            return 0;
        }

        enum DateUnit unit = parseDateUnit(values[0]);
        if (unit == DATE_UNIT_INVALID)
        {
            return 0;
        }

        struct DateTime dt = {0};
        int is_datetime = 0;
        if (parseDateTime(values[1], &dt))
        {
            is_datetime = 1;
        }
        else if (!parseDate(values[1], &dt))
        {
            return 0;
        }

        dateTrunc(&dt, unit);

        if (is_datetime)
        {
            return sprintDateTime(output, &dt);
        }
        return sprintDate(output, &dt);
    }
    else if (function == FUNC_AT_TIME_ZONE)
    {
        if (value_count < 2)
        {
            return 0;
        }

        struct DateTime dt = {0};
        if (!parseDateTime(values[0], &dt))
        {
            return 0;
        }

        if (dt.tz_offset_minutes == TZ_LOCAL)
        {
            // Cannot convert an untagged local datetime
            output[0] = '\0';
            return 0;
        }

        short target = 0;
        int as_local = 0;
        if (strcmp(values[1], "LOCAL") == 0)
        {
            if (!getLocalTzOffsetMinutes(&target))
            {
                output[0] = '\0';
                return 0;
            }
            as_local = 1;
        }
        else if (!parseTzOffset(values[1], &target))
        {
            return 0;
        }

        if (!datetimeAtTimeZone(&dt, target))
        {
            output[0] = '\0';
            return 0;
        }

        // LOCAL / explicit ±offsets use ±HH:MM; UTC/Z keep trailing Z
        if (as_local ||
            (values[1][0] == '+' || values[1][0] == '-'))
        {
            dt.tz_format = TZ_FMT_OFFSET;
        }
        else
        {
            dt.tz_format = TZ_FMT_Z;
        }

        return sprintDateTime(output, &dt);
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
        dt.hour = 0;
        dt.minute = 0;
        dt.second = 0;
        dt.tz_offset_minutes = TZ_LOCAL;
        dt.tz_format = TZ_FMT_Z;

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
        struct DateTime dt;
        // Default to 0
        dt.year = 0;
        // Default to 1 (same as MAKE_DATE)
        dt.month = 1;
        // Default to 1 (same as MAKE_DATE)
        dt.day = 1;
        // Default to 0
        dt.hour = 0;
        dt.minute = 0;
        dt.second = 0;
        dt.tz_offset_minutes = TZ_LOCAL;
        dt.tz_format = TZ_FMT_Z;

        if (value_count > 0)
        {
            dt.year = atoi(values[0]);

            if (value_count > 1)
            {
                dt.month = atoi(values[1]);

                if (value_count > 2)
                {
                    dt.day = atoi(values[2]);

                    if (value_count > 3)
                    {
                        dt.hour = atoi(values[3]);

                        if (value_count > 4)
                        {
                            dt.minute = atoi(values[4]);

                            if (value_count > 5)
                            {
                                dt.second = atoi(values[5]);
                            }
                        }
                    }
                }
            }
        }

        return sprintDateTime(output, &dt);
    }

    fprintf(stderr, "Unable to evaluate function %d\n", function);
    return -1;
}

typedef char LongestValue[MAX_VALUE_LENGTH];

static int cmp_string_ptrs(const void *a, const void *b)
{
    const char *sa = *(const char *const *)a;
    const char *sb = *(const char *const *)b;
    return strcmp(sa, sb);
}

static int cmp_numeric_ptrs(const void *a, const void *b)
{
    const char *sa = *(const char *const *)a;
    const char *sb = *(const char *const *)b;
    return compare_numeric(sa, sb);
}

/**
 * Evaluate the percentile fraction (second arg) into [0, 1].
 * Returns 0 on success, -1 if missing/invalid/out of range.
 */
static int agg_eval_percentile_p(
    struct Table *tables,
    RowListIndex list_id,
    struct Node *node,
    double *p_out)
{
    if (node->child_count < 2)
    {
        return -1;
    }

    char p_buf[MAX_VALUE_LENGTH] = {0};
    struct Node *p_node = &node->children[1];

    if (isConstantNode(p_node))
    {
        if (evaluateConstantNode(p_node, p_buf) < 0)
        {
            return -1;
        }
    }
    else
    {
        struct RowList *rl = getRowList(list_id);
        if (rl == NULL || rl->row_count == 0)
        {
            return -1;
        }
        evaluateNode(
            tables,
            list_id,
            0,
            p_node,
            p_buf,
            MAX_VALUE_LENGTH);
    }

    if (p_buf[0] == '\0')
    {
        return -1;
    }

    char *end = NULL;
    double p = strtod(p_buf, &end);
    if (end == p_buf || *end != '\0' || p < 0.0 || p > 1.0)
    {
        return -1;
    }

    *p_out = p;
    return 0;
}

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

    // Aggregates collect the first expression argument per row. Extra args are
    // allowed (e.g. PERCENTILE(expr, p)); functions that only use one
    // expression simply ignore children beyond [0].

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
                    // Pack included values densely at [0..row_count) so later
                    // passes over row_count see every filtered row (FILTER can
                    // skip earlier indices).
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
                            values[row_count],
                            MAX_VALUE_LENGTH);
                    }
                    else if (node->child_count >= 1)
                    {
                        // First child is the aggregated expression; further
                        // children (e.g. percentile fraction) are read later.
                        evaluateNode(
                            tables,
                            list_id,
                            i,
                            &node->children[0],
                            values[row_count],
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
        double min = DBL_MAX;
        int found = 0;

        for (int i = 0; i < row_count; i++)
        {
            // Only consider non-NULL real values (complex skipped)
            if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
            {
                double v = strtod(values[i], NULL);

                if (!found || v < min)
                    min = v;
                found = 1;
            }
        }

        if (found)
        {
            bytesWritten = sprint_number(output, min);
        }
    }
    else if (node->function == FUNC_AGG_MAX)
    {
        double max = -DBL_MAX;
        int found = 0;

        for (int i = 0; i < row_count; i++)
        {
            // Only consider non-NULL real values (complex skipped)
            if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
            {
                double v = strtod(values[i], NULL);

                if (!found || v > max)
                    max = v;
                found = 1;
            }
        }

        if (found)
        {
            bytesWritten = sprint_number(output, max);
        }
    }
    else if (node->function == FUNC_AGG_SUM)
    {
        int non_null = 0;
        int any_complex = 0;

        for (int i = 0; i < row_count; i++)
        {
            if (IS_NOT_NULL(values[i]) && is_complex(values[i]))
            {
                any_complex = 1;
                break;
            }
        }

        if (any_complex)
        {
            double complex sum = 0;
            for (int i = 0; i < row_count; i++)
            {
                if (IS_NOT_NULL(values[i]))
                {
                    double complex z;
                    if (!parse_complex_value(values[i], &z))
                        continue;
                    non_null = 1;
                    sum += z;
                }
            }
            if (non_null)
            {
                bytesWritten = sprint_complex(output, creal(sum), cimag(sum));
            }
        }
        else
        {
            double sum = 0;

            for (int i = 0; i < row_count; i++)
            {
                // Sum the non-NULL values
                if (IS_NOT_NULL(values[i]))
                {
                    non_null = 1;
                    sum += strtod(values[i], NULL);
                }
            }

            // If *all* rows are NULL then the result is NULL
            if (non_null)
            {
                bytesWritten = sprint_number(output, sum);
            }
        }
    }
    else if (node->function == FUNC_AGG_AVG)
    {
        int count = 0;
        int any_complex = 0;

        for (int i = 0; i < row_count; i++)
        {
            if (IS_NOT_NULL(values[i]) && is_complex(values[i]))
            {
                any_complex = 1;
                break;
            }
        }

        if (any_complex)
        {
            double complex sum = 0;
            for (int i = 0; i < row_count; i++)
            {
                if (IS_NOT_NULL(values[i]))
                {
                    double complex z;
                    if (!parse_complex_value(values[i], &z))
                        continue;
                    count++;
                    sum += z;
                }
            }
            if (count > 0)
            {
                double complex avg = sum / count;
                bytesWritten = sprint_complex(output, creal(avg), cimag(avg));
            }
        }
        else
        {
            double sum = 0;

            for (int i = 0; i < row_count; i++)
            {
                // Count up the non-NULL values
                if (IS_NOT_NULL(values[i]))
                {
                    count++;

                    sum += strtod(values[i], NULL);
                }
            }

            if (count > 0)
            {
                bytesWritten = sprint_number(output, sum / count);
            }
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
    else if (
        node->function == FUNC_AGG_STDDEV_SAMP ||
        node->function == FUNC_AGG_STDDEV_POP ||
        node->function == FUNC_AGG_VAR_SAMP ||
        node->function == FUNC_AGG_VAR_POP)
    {
        // Two-pass mean / sum-of-squared-deviations over non-NULL reals.
        // Complex values are ignored (same as MIN/MAX).
        int count = 0;
        double sum = 0;

        for (int i = 0; i < row_count; i++)
        {
            if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
            {
                count++;
                sum += strtod(values[i], NULL);
            }
        }

        int is_sample =
            node->function == FUNC_AGG_STDDEV_SAMP ||
            node->function == FUNC_AGG_VAR_SAMP;

        // Sample needs ≥2 observations; population needs ≥1.
        if ((is_sample && count >= 2) || (!is_sample && count >= 1))
        {
            double mean = sum / count;
            double ss = 0;

            for (int i = 0; i < row_count; i++)
            {
                if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
                {
                    double d = strtod(values[i], NULL) - mean;
                    ss += d * d;
                }
            }

            double denom = is_sample ? (double)(count - 1) : (double)count;
            double variance = ss / denom;

            if (
                node->function == FUNC_AGG_STDDEV_SAMP ||
                node->function == FUNC_AGG_STDDEV_POP)
            {
                bytesWritten = sprint_number(output, sqrt(variance));
            }
            else
            {
                bytesWritten = sprint_number(output, variance);
            }
        }
    }
    else if (node->function == FUNC_AGG_MEDIAN)
    {
        // Discrete median: sort non-NULL reals, return the value at index
        // (n-1)/2 (lower middle when n is even — always a member of the set).
        int count = 0;
        const char **ptrs = malloc((size_t)row_count * sizeof(*ptrs));

        if (ptrs == NULL)
        {
            fprintf(stderr, "Unable to allocate for MEDIAN\n");
            exit(-1);
        }

        for (int i = 0; i < row_count; i++)
        {
            if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
            {
                ptrs[count++] = values[i];
            }
        }

        if (count > 0)
        {
            qsort(ptrs, (size_t)count, sizeof(*ptrs), cmp_numeric_ptrs);
            bytesWritten = sprintf(output, "%s", ptrs[(count - 1) / 2]);
        }

        free(ptrs);
    }
    else if (node->function == FUNC_AGG_MODE)
    {
        // Mode via string equality (works for text and identical numeric
        // spellings). Ties → lexicographically smallest value.
        int count = 0;
        const char **ptrs = malloc((size_t)row_count * sizeof(*ptrs));

        if (ptrs == NULL)
        {
            fprintf(stderr, "Unable to allocate for MODE\n");
            exit(-1);
        }

        for (int i = 0; i < row_count; i++)
        {
            if (IS_NOT_NULL(values[i]))
            {
                ptrs[count++] = values[i];
            }
        }

        if (count > 0)
        {
            qsort(ptrs, (size_t)count, sizeof(*ptrs), cmp_string_ptrs);

            const char *best = ptrs[0];
            int best_run = 1;
            int run = 1;

            for (int i = 1; i < count; i++)
            {
                if (strcmp(ptrs[i], ptrs[i - 1]) == 0)
                {
                    run++;
                }
                else
                {
                    run = 1;
                }

                // Strict > keeps the first (lexicographically smallest) on ties
                if (run > best_run)
                {
                    best_run = run;
                    best = ptrs[i];
                }
            }

            bytesWritten = sprintf(output, "%s", best);
        }

        free(ptrs);
    }
    else if (
        node->function == FUNC_AGG_PERCENTILE_CONT ||
        node->function == FUNC_AGG_PERCENTILE_DISC)
    {
        double p = 0;
        if (agg_eval_percentile_p(tables, list_id, node, &p) == 0)
        {
            int count = 0;
            const char **ptrs = malloc((size_t)row_count * sizeof(*ptrs));

            if (ptrs == NULL)
            {
                fprintf(stderr, "Unable to allocate for PERCENTILE\n");
                exit(-1);
            }

            for (int i = 0; i < row_count; i++)
            {
                if (IS_NOT_NULL(values[i]) && !is_complex(values[i]))
                {
                    ptrs[count++] = values[i];
                }
            }

            if (count > 0)
            {
                qsort(ptrs, (size_t)count, sizeof(*ptrs), cmp_numeric_ptrs);

                if (node->function == FUNC_AGG_PERCENTILE_DISC)
                {
                    // Discrete: value at ceil(n*p)-1 (p=0 → first).
                    // Matches MEDIAN at p=0.5 (index (n-1)/2).
                    int idx;
                    if (p <= 0.0)
                    {
                        idx = 0;
                    }
                    else
                    {
                        idx = (int)ceil(p * (double)count) - 1;
                        if (idx < 0)
                            idx = 0;
                        if (idx >= count)
                            idx = count - 1;
                    }
                    bytesWritten = sprintf(output, "%s", ptrs[idx]);
                }
                else
                {
                    // Continuous: interpolate at (n-1)*p.
                    double h = p * (double)(count - 1);
                    int lo = (int)floor(h);
                    int hi = (int)ceil(h);
                    if (lo < 0)
                        lo = 0;
                    if (hi >= count)
                        hi = count - 1;

                    if (lo == hi)
                    {
                        bytesWritten = sprintf(output, "%s", ptrs[lo]);
                    }
                    else
                    {
                        double a = strtod(ptrs[lo], NULL);
                        double b = strtod(ptrs[hi], NULL);
                        double frac = h - (double)lo;
                        bytesWritten = sprint_number(output, a + (b - a) * frac);
                    }
                }
            }

            free(ptrs);
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
