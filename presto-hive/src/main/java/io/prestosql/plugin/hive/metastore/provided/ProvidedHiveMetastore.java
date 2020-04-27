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
package io.prestosql.plugin.hive.metastore.provided;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.prestosql.plugin.hive.util.HiveBucketing;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.session.metadata.BucketMetadata;
import io.prestosql.spi.session.metadata.ColumnMetadata;
import io.prestosql.spi.session.metadata.PartitionInfo;
import io.prestosql.spi.session.metadata.PartitionMetadata;
import io.prestosql.spi.session.metadata.QueryRequestMetadata;
import io.prestosql.spi.session.metadata.StatisticsProperties;
import io.prestosql.spi.session.metadata.StorageMetadata;
import io.prestosql.spi.session.metadata.TableMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.prestosql.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.prestosql.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.prestosql.plugin.hive.HiveMetadata.SKIP_FOOTER_COUNT_KEY;
import static io.prestosql.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.prestosql.plugin.hive.HiveTableProperties.AVRO_SCHEMA_URL;
import static io.prestosql.plugin.hive.HiveTableProperties.CSV_ESCAPE;
import static io.prestosql.plugin.hive.HiveTableProperties.CSV_QUOTE;
import static io.prestosql.plugin.hive.HiveTableProperties.CSV_SEPARATOR;
import static io.prestosql.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static io.prestosql.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.prestosql.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.prestosql.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.prestosql.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR;
import static io.prestosql.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR_ESCAPE;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_BUCKETING_VERSION;

public class ProvidedHiveMetastore
        implements HiveMetastore
{
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return Optional.of(Database.builder()
                .setDatabaseName(Database.DEFAULT_DATABASE_NAME)
                .build());
    }

    @Override
    public List<String> getAllDatabases()
    {
        return ImmutableList.of(Database.DEFAULT_DATABASE_NAME);
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        Optional<QueryRequestMetadata> metadata = identity.getMetadata();

        if (!metadata.isPresent()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Metadata is not present");
        }

        TableMetadata table = metadata.get().getMetadata().stream().filter(f -> f.getName().equals(tableName)).collect(MoreCollectors.onlyElement());

        Table.Builder builder = Table.builder()
                .setDatabaseName(Database.DEFAULT_DATABASE_NAME)
                .setTableName(table.getName())
                .setOwner(table.getOwner())
                .setTableType(table.getType().toString())
                .setDataColumns(table.getDataColumns().stream()
                        .map(dataColumn -> new Column(dataColumn.getName(), HiveType.valueOf(dataColumn.getDataType()), dataColumn.getComment()))
                        .collect(toImmutableList()))
                .setPartitionColumns(table.getPartitions().orElse(ImmutableList.of()).stream()
                        .map(PartitionMetadata::getColumns)
                        .flatMap(f -> f.stream())
                        .distinct()
                        .map(column -> new Column(column.getName(), HiveType.valueOf(column.getDataType()), column.getComment()))
                        .collect(toImmutableList()))
                .setParameters(toHivePropertiesFormat(table.getAdditionalProperties().orElse(ImmutableMap.of())))
                .setViewOriginalText(Optional.empty())
                .setViewExpandedText(Optional.empty());

        StorageMetadata storage = table.getStorage().get();

        builder.getStorageBuilder()
                .setSkewed(storage.isSkewed())
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.valueOf(storage.getFormat().toUpperCase(ENGLISH))))
                .setLocation(storage.getLocation())
                .setBucketProperty(storage.getBucket().isPresent() ?
                        fromStorageDescriptor(storage.getBucket().get(), tableName) :
                        Optional.empty())
                .setSerdeParameters(toHivePropertiesFormat(storage.getSerdeProperties().orElse(ImmutableMap.of())));
        return Optional.ofNullable(builder.build());
    }

    private Map<String, String> toHivePropertiesFormat(Map<String, String> properties)
    {
        if (properties.size() == 0) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Entry<String, String> property : properties.entrySet()) {
            switch (property.getKey()) {
                case ORC_BLOOM_FILTER_COLUMNS:
                    builder.put(ORC_BLOOM_FILTER_COLUMNS_KEY, property.getValue());
                    break;
                case ORC_BLOOM_FILTER_FPP:
                    builder.put(ORC_BLOOM_FILTER_FPP_KEY, property.getValue());
                    break;
                case AVRO_SCHEMA_URL:
                    builder.put(AVRO_SCHEMA_URL_KEY, property.getValue());
                    break;
                case TEXTFILE_FIELD_SEPARATOR:
                    builder.put(serdeConstants.FIELD_DELIM, property.getValue());
                    break;
                case TEXTFILE_FIELD_SEPARATOR_ESCAPE:
                    builder.put(serdeConstants.ESCAPE_CHAR, property.getValue());
                    break;
                case SKIP_HEADER_LINE_COUNT:
                    builder.put(SKIP_HEADER_COUNT_KEY, property.getValue());
                    break;
                case SKIP_FOOTER_LINE_COUNT:
                    builder.put(SKIP_FOOTER_COUNT_KEY, property.getValue());
                    break;
                case CSV_SEPARATOR:
                    builder.put(OpenCSVSerde.SEPARATORCHAR, property.getValue());
                    break;
                case CSV_QUOTE:
                    builder.put(OpenCSVSerde.QUOTECHAR, property.getValue());
                    break;
                case CSV_ESCAPE:
                    builder.put(OpenCSVSerde.ESCAPECHAR, property.getValue());
                    break;
                default:
                    builder.put(property.getKey(), property.getValue());
            }
        }

        return builder.build();
    }

    public Optional<HiveBucketProperty> fromStorageDescriptor(BucketMetadata bucket, String tablePartitionName)
    {
        boolean bucketColsSet = bucket.getBy().isPresent() && bucket.getBy().get().size() > 0;
        boolean numBucketsSet = bucket.getCount().isPresent();
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }
        if (!bucketColsSet) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
        }
        List<SortingColumn> sortedBy = ImmutableList.of();
        if (bucket.getSortedBy().isPresent()) {
            sortedBy = bucket.getSortedBy().orElse(ImmutableList.of()).stream()
                    .map(column -> SortingColumn.fromMetastoreApiOrder(
                            new Order(column.getColumnName(), SortingColumn.Order.valueOf(column.getOrder()).getHiveOrder()),
                            tablePartitionName))
                    .collect(toImmutableList());
        }
        HiveBucketing.BucketingVersion bucketingVersion = HiveBucketing.getBucketingVersion(ImmutableMap.of(TABLE_BUCKETING_VERSION, bucket.getVersion().toString()));
        return Optional.of(new HiveBucketProperty(bucket.getBy().get(), bucketingVersion, bucket.getCount().get(), sortedBy));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        try {
            Optional<QueryRequestMetadata> metadata = identity.getMetadata();
            TableMetadata tableMetadata = metadata.get().getMetadata().stream().filter(f -> f.getName().equals(table.getTableName())).collect(MoreCollectors.onlyElement());

            HiveBasicStatistics basicStats = ThriftMetastoreUtil.getHiveBasicStatistics(table.getParameters());
            List<Column> columns = new ArrayList<>(table.getPartitionColumns());
            columns.addAll(table.getDataColumns());

            ImmutableMap.Builder<String, HiveColumnStatistics> builder = new ImmutableMap.Builder();
            tableMetadata.getDataColumns().forEach(col -> builder.put(col.getName(), getHiveColumnStatistics(col)));

            return new PartitionStatistics(basicStats, builder.build());
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private HiveColumnStatistics getHiveColumnStatistics(ColumnMetadata col)
    {
        if (!col.getStatistics().isPresent()) {
            return HiveColumnStatistics.empty();
        }

        Map<String, Object> stats = col.getStatistics().get().getStatistics();

        if (HiveType.HIVE_BYTE.equals(HiveType.valueOf(col.getDataType()))
                || HiveType.HIVE_SHORT.equals(HiveType.valueOf(col.getDataType()))
                || HiveType.HIVE_INT.equals(HiveType.valueOf(col.getDataType()))
                || HiveType.HIVE_LONG.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createIntegerColumnStatistics(
                    stats.containsKey(StatisticsProperties.MIN_NUMERIC) ? OptionalLong.of((Long) stats.get(StatisticsProperties.MIN_NUMERIC)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.MAX_NUMERIC) ? OptionalLong.of((Long) stats.get(StatisticsProperties.MAX_NUMERIC)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.DISTINCT_VALUES_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.DISTINCT_VALUES_COUNT)) : OptionalLong.empty());
        }
        else if (HiveType.HIVE_FLOAT.equals(HiveType.valueOf(col.getDataType()))
                || HiveType.HIVE_DOUBLE.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createDoubleColumnStatistics(
                    stats.containsKey(StatisticsProperties.MIN_NUMERIC) ? OptionalDouble.of((Long) stats.get(StatisticsProperties.MIN_NUMERIC)) : OptionalDouble.empty(),
                    stats.containsKey(StatisticsProperties.MAX_NUMERIC) ? OptionalDouble.of((Long) stats.get(StatisticsProperties.MAX_NUMERIC)) : OptionalDouble.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.DISTINCT_VALUES_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.DISTINCT_VALUES_COUNT)) : OptionalLong.empty());
        }
        else if (HiveType.HIVE_DATE.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createDateColumnStatistics(
                    stats.containsKey(StatisticsProperties.MIN_DATE) ? Optional.of(LocalDate.parse(stats.get(StatisticsProperties.MIN_DATE).toString())) : Optional.empty(),
                    stats.containsKey(StatisticsProperties.MAX_DATE) ? Optional.of(LocalDate.parse(stats.get(StatisticsProperties.MAX_DATE).toString())) : Optional.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.DISTINCT_VALUES_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.DISTINCT_VALUES_COUNT)) : OptionalLong.empty());
        }
        else if (HiveType.HIVE_BOOLEAN.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createBooleanColumnStatistics(
                    stats.containsKey(StatisticsProperties.TRUE_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.TRUE_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.FALSE_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.FALSE_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty());
        }
        else if (HiveType.HIVE_BINARY.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createBinaryColumnStatistics(
                    stats.containsKey(StatisticsProperties.MAX_VALUE_SIZE_IN_BYTES) ? OptionalLong.of((Long) stats.get(StatisticsProperties.MAX_VALUE_SIZE_IN_BYTES)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.TOTAL_SIZE_IN_BYTES) ? OptionalLong.of((Long) stats.get(StatisticsProperties.TOTAL_SIZE_IN_BYTES)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty());
        }
        else if (HiveType.HIVE_STRING.equals(HiveType.valueOf(col.getDataType()))) {
            return HiveColumnStatistics.createStringColumnStatistics(
                    stats.containsKey(StatisticsProperties.MAX_VALUE_SIZE_IN_BYTES) ? OptionalLong.of((Long) stats.get(StatisticsProperties.MAX_VALUE_SIZE_IN_BYTES)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.TOTAL_SIZE_IN_BYTES) ? OptionalLong.of((Long) stats.get(StatisticsProperties.TOTAL_SIZE_IN_BYTES)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.NULLS_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.NULLS_COUNT)) : OptionalLong.empty(),
                    stats.containsKey(StatisticsProperties.DISTINCT_VALUES_COUNT) ? OptionalLong.of((Long) stats.get(StatisticsProperties.DISTINCT_VALUES_COUNT)) : OptionalLong.empty());
        }
        else {
            throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + stats);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new PrestoException(NOT_SUPPORTED, "updateTableStatistics");
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new PrestoException(NOT_SUPPORTED, "updatePartitionStatistics");
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return new ArrayList<>(0);
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return new ArrayList<>(0);
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return new ArrayList<>(0);
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        throw new PrestoException(NOT_SUPPORTED, "createDatabase");
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropDatabase");
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        throw new PrestoException(NOT_SUPPORTED, "renameDatabase");
    }

    @Override
    public void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "setDatabaseOwner");
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "createTable");
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropTable");
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "replaceTable");
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "renameTable");
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        throw new PrestoException(NOT_SUPPORTED, "commentTable");
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new PrestoException(NOT_SUPPORTED, "addColumn");
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new PrestoException(NOT_SUPPORTED, "renameColumn");
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropColumn");
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        throw new PrestoException(NOT_SUPPORTED, "getPartition");
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "getPartitionNames");
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        Optional<QueryRequestMetadata> metadata = identity.getMetadata();
        TableMetadata tableMetadata = metadata.get().getMetadata().stream().filter(f -> f.getName().equals(tableName)).collect(MoreCollectors.onlyElement());

        List<PartitionMetadata> partitions = tableMetadata.getPartitions().get().stream()
                .filter(partition -> partition.getInfos().stream().map(PartitionInfo::getValues).flatMap(f -> f.stream())
                        .anyMatch(p -> emptyParts(parts) || parts.contains(p)))
                .collect(Collectors.toList());

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        for (PartitionMetadata partitionMetadata : partitions) {
            int size = partitionMetadata.getColumns().size();
            List<ColumnMetadata> columns = partitionMetadata.getColumns();
            List<String> values = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ColumnMetadata column = columns.get(i);

                List<PartitionInfo> infos = partitionMetadata.getInfos().stream().filter(f ->
                        f.getValues().stream().anyMatch(a -> emptyParts(parts) || parts.contains(a))).collect(Collectors.toList());
                int infoSize = infos.size();
                for (int j = 0; j < infoSize; j++) {
                    PartitionInfo info = infos.get(j);
                    if (i == 0) {
                        values.add(String.join("=", column.getName(), info.getValues().get(i)));
                    }
                    else {
                        values.set(j, String.join("/", values.get(j), "=".join(column.getName(), info.getValues().get(i))));
                    }
                }
            }
            builder.addAll(values);
        }

        return Optional.of(builder.build());
    }

    private boolean emptyParts(List<String> parts)
    {
        return parts.isEmpty() || (parts.size() == 1 && parts.get(0).isEmpty());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        Optional<QueryRequestMetadata> metadata = identity.getMetadata();
        TableMetadata tableMetadata = metadata.get().getMetadata().stream().filter(f -> f.getName().equals(table.getTableName())).collect(MoreCollectors.onlyElement());

        List<PartitionMetadata> partitions = tableMetadata.getPartitions().orElse(ImmutableList.of());

        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (PartitionMetadata partition : partitions) {
            List<PartitionInfo> infos = partition.getInfos().stream().filter(f -> f.getValues().stream().anyMatch(a ->
                    partitionNames.stream().anyMatch(p -> p.endsWith(a)))).collect(Collectors.toList());
            for (PartitionInfo info : infos) {
                Partition.Builder partitionBuilder = Partition.builder()
                        .setColumns(tableMetadata.getDataColumns().stream()
                                .map(dataColumn -> new Column(dataColumn.getName(), HiveType.valueOf(dataColumn.getDataType()), dataColumn.getComment()))
                                .collect(toImmutableList()))
                        .setDatabaseName(table.getDatabaseName())
                        .setParameters(partition.getColumns().stream()
                                .flatMap(f -> f.getProperties().orElse(ImmutableMap.of()).entrySet().stream())
                                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
                        .setValues(info.getValues())
                        .setTableName(table.getTableName());

                partitionBuilder.getStorageBuilder()
                        .setSkewed(info.getStorage().isSkewed())
                        .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.valueOf(info.getStorage().getFormat().toUpperCase(ENGLISH))))
                        .setLocation(info.getStorage().getLocation())
                        .setBucketProperty(info.getStorage().getBucket().isPresent() ?
                                fromStorageDescriptor(info.getStorage().getBucket().get(), table.getTableName()) :
                                Optional.empty())
                        .setSerdeParameters(toHivePropertiesFormat(info.getStorage().getSerdeProperties().orElse(ImmutableMap.of())));

                int size = partition.getColumns().size();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < size; i++) {
                    if (i > 0) {
                        sb.append("/");
                    }
                    sb.append(String.join("=", partition.getColumns().get(i).getName(), info.getValues().get(i)));
                }
                builder.put(sb.toString(), Optional.of(partitionBuilder.build()));
            }
        }

        return builder.build();
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new PrestoException(NOT_SUPPORTED, "addPartitions");
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropPartition");
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new PrestoException(NOT_SUPPORTED, "alterPartition");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "createRole");
    }

    @Override
    public void dropRole(String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropRole");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new PrestoException(NOT_SUPPORTED, "listRoles");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantRoles");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeRoles");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "listRoleGrants");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantTablePrivileges");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeTablePrivileges");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "listTablePrivileges");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
