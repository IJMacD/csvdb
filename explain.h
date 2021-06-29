

int explain_select_query (
    const char *table,
    const char *fields,
    int field_count,
    int flags,
    int offset_value,
    int limit_value,
    const char *predicate_field,
    char predicate_op,
    const char *predicate_value,
    const char *order_field,
    int order_direction,
    int output_flags
);