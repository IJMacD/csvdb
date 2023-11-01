#include <stdlib.h>

#include "../structs.h"
#include "../query/result.h"
#include "../query/output.h"

int executeSelect (
    FILE *output,
    enum OutputOption options,
    struct Table *tables,
    int table_count,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    int row_count = 0;

    /*************************
     * Output headers
     ************************/
    printPreamble(output, NULL, 0, step->nodes, step->node_count, options);

    if (options & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(
            output,
            tables,
            table_count,
            step->nodes,
            step->node_count,
            options
        );
    }

    /*******************
    * Output result set
    *******************/

    RowListIndex list_id;

    while ((list_id = popRowList(result_set)) >= 0) {
        struct RowList *row_list = getRowList(list_id);

        // Aggregate functions will print just one row
        if (row_list->group) {
            printResultLine(
                output,
                tables,
                table_count,
                step->nodes,
                step->node_count,
                row_count,
                list_id,
                options
            );
            row_count++;
        }
        else for (unsigned int i = 0; i < row_list->row_count; i++) {
            printResultLine(
                output,
                tables,
                table_count,
                step->nodes,
                step->node_count,
                i,
                list_id,
                options
            );
            row_count++;
        }

        destroyRowList(list_id);
    }

    printPostamble(
        output,
        NULL,
        0,
        step->nodes,
        step->node_count,
        row_count,
        options
    );

    return 0;
}