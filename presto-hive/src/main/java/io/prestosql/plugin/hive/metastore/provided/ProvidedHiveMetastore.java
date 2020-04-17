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
import io.prestosql.spi.session.metadata.QueryRequestMetadata;
import io.prestosql.spi.session.metadata.StatisticsProperties;
import io.prestosql.spi.session.metadata.StorageMetadata;
import io.prestosql.spi.session.metadata.TableMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.Order;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
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
                .setOwner(null)
                .setTableType(table.getType().toString())
                .setDataColumns(table.getDataColumns().stream()
                        .map(dataColumn -> new Column(dataColumn.getName(), HiveType.valueOf(dataColumn.getDataType()), dataColumn.getComment()))
                        .collect(toImmutableList()))
                .setPartitionColumns(table.getPartitions().orElse(ImmutableList.of()).stream()
                        .map(partition -> new Column(partition.getName(), HiveType.valueOf(partition.getDataType()), partition.getComment()))
                        .collect(toImmutableList()))
                .setParameters(table.getAdditionalProperties().orElse(ImmutableMap.of()))
                .setViewOriginalText(Optional.empty())
                .setViewExpandedText(Optional.empty());

        StorageMetadata storage = table.getStorage().get();

        builder.getStorageBuilder()
                .setSkewed(storage.isSkewed())
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.valueOf(storage.getFormat())))
                .setLocation(storage.getLocation())
                .setBucketProperty(storage.getBucket().isPresent() ?
                        fromStorageDescriptor(storage.getBucket().get(), tableName) :
                        Optional.empty())
                .setSerdeParameters(ImmutableMap.of());
        return Optional.ofNullable(builder.build());
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
        return null;
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {

    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {

    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return null;
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return null;
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return null;
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {

    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {

    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {

    }

    @Override
    public void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {

    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {

    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {

    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {

    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {

    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {

    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {

    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {

    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {

    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        return null;
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {

    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {

    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {

    }

    @Override
    public void createRole(String role, String grantor)
    {

    }

    @Override
    public void dropRole(String role)
    {

    }

    @Override
    public Set<String> listRoles()
    {
        return null;
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {

    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {

    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return null;
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {

    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {

    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal)
    {
        return null;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
