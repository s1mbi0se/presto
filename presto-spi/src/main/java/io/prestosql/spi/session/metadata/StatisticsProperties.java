package io.prestosql.spi.session.metadata;

public class StatisticsProperties
{
    public static final String MIN_NUMERIC = "min";
    public static final String MIN_DATE = "min_date";
    public static final String MAX_NUMERIC = "max";
    public static final String MAX_DATE = "max_date";
    public static final String NULLS_COUNT = "nulls_count";
    public static final String DISTINCT_VALUES_COUNT = "distinct_values_count";
    public static final String TRUE_COUNT = "true_count";
    public static final String FALSE_COUNT = "false_count";
    public static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    public static final String MAX_VALUE_SIZE_IN_BYTES = "max_value_size_in_bytes";
    public static final String NUM_FILES = "num_files";
    public static final String NUM_ROWS = "num_rows";
    public static final String RAW_DATA_SIZE = "raw_data_size";
    public static final String TOTAL_SIZE = "total_size";
}
