/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    private StatisticsProperties()
    {
    }
}
