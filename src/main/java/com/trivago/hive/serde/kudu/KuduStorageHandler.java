/*
  Copyright 2016 Bimal Tandel

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.trivago.hive.serde.kudu;

import com.trivago.hive.serde.kudu.input.KuduTableInputFormatWrapper;
import com.trivago.hive.serde.kudu.output.HiveKuduOutputFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * Created by bimal on 4/11/16.
 */

@SuppressWarnings({"RedundantThrows", "deprecation"})
public class KuduStorageHandler extends DefaultStorageHandler
    implements HiveMetaHook, HiveStoragePredicateHandler {

  private static final Log LOG = LogFactory.getLog(KuduStorageHandler.class);

  private Configuration conf;


  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KuduTableInputFormatWrapper.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveKuduOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return HiveKuduSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  private void configureJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {

    Properties tblProps = tableDesc.getProperties();
    copyPropertiesFromTable(jobProperties, tblProps);

    // set extra configuration properties for kudu-mapreduce
    for (String key : tblProps.stringPropertyNames()) {
      if (key.startsWith(HiveKuduConstants.MR_PROPERTY_PREFIX)) {
        String value = tblProps.getProperty(key);
        jobProperties.put(key, value);
        //Also set configuration for Non Map Reduce Hive calls to the Handler
        conf.set(key, value);
      }
    }
  }

  private void copyPropertiesFromTable(Map<String, String> jobProperties, Properties tblProps) {
    for (Map.Entry<String, String> propToCopy : HiveKuduConstants.KUDU_TO_MAPREDUCE_MAPPING.entrySet()) {
      if (tblProps.containsKey(propToCopy.getValue())) {
        String value = tblProps.getProperty(propToCopy.getValue());
        conf.set(propToCopy.getKey(), value);
        jobProperties.put(propToCopy.getKey(), value);
      }
    }
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
      throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
      Deserializer deserializer, ExprNodeDesc predicate) {

    return KuduPredicateAnalyzer.decomposePredicate(jobConf, predicate);
  }

  private String getKuduTableName(Table tbl) {

    String tableName;
    tableName = conf.get(HiveKuduConstants.TABLE_NAME);
    if (tableName == null) {
      LOG.info("searching for " + HiveKuduConstants.TABLE_NAME + " in table parameters");
      tableName = tbl.getParameters().get(HiveKuduConstants.TABLE_NAME);
      if (tableName == null) {
        LOG.warn("Kudu Table name was not provided in table properties.");
        LOG.warn("Attempting to use Hive Table name");
        tableName = tbl.getTableName().replaceAll(".*\\.", "");
        LOG.warn("Kudu Table name will be: " + tableName);
      }

    }
    return tableName;
  }


  @Override
  public void preCreateTable(Table tbl)
      throws MetaException {

    String kuduTableName = getKuduTableName(tbl);

    KuduClient client = HiveKuduBridgeUtils
        .getKuduClient(conf, tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
    List<FieldSchema> tabColumns = tbl.getSd().getCols();

    try {
      boolean tableExists = client.tableExists(kuduTableName);
      if (isExternal) {
        if (!tableExists) {
          throw new MetaException("Tried to create external table on non-existing Kudu table");
        }
        // our SerDe builds the ObjectInspector based on the column information stored in the Kudu MetaStore,
        // so any columns in the create table statement will be ignored for External tables.
        return;
      }
      else if (tableExists)
        throw new MetaException("Tried to create non-external table on existing Kudu table");
    } catch (KuduException e) {
      throw new MetaException("Failed to connect to kudu" + e);
    }


    int numberOfCols = tabColumns.size();

    List<ColumnSchema> columns = new ArrayList<>(numberOfCols);
    List<String> keyColumns = Arrays.asList(tbl.getParameters()
        .get(HiveKuduConstants.KEY_COLUMNS).split("\\s*,\\s*"));

    try {
      for (FieldSchema fields : tabColumns) {

        ColumnSchemaBuilder columnSchemaBuilder = new ColumnSchema
            .ColumnSchemaBuilder(fields.getName(), HiveKuduBridgeUtils.hiveTypeToKuduType(fields.getType()))
            .key(keyColumns.contains(fields.getName()))
            .nullable(!keyColumns.contains(fields.getName()));

        if (fields.getType().toLowerCase(Locale.US).startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
          String fieldType = fields.getType().toLowerCase(Locale.US);
          String[] decimal = fieldType.replace(serdeConstants.DECIMAL_TYPE_NAME, StringUtils.EMPTY).replaceAll("[()]", StringUtils.EMPTY)
              .split(",");
          ColumnTypeAttributes columnTypeAttributes = new ColumnTypeAttributesBuilder()
              .precision(Integer.parseInt(decimal[0])).scale(Integer.parseInt(decimal[1])).build();
          columnSchemaBuilder.typeAttributes(columnTypeAttributes);
        }

        ColumnSchema columnSchema = columnSchemaBuilder.build();
        columns.add(columnSchema);
      }

      Schema schema = new Schema(columns);

      CreateTableOptions createTableOptions = new CreateTableOptions();

      // adding partitions
      String[] partitionColumns = tbl.getParameters().get(HiveKuduConstants.PARTITION_COLUMNS).split("\\s*,\\s*");

      for (String partitionColumn : partitionColumns) {
        if (!keyColumns.contains(partitionColumn)) {
          throw new MetaException("all partition columns need to be part of the key column!");
        }

        Integer numberBuckets;
        if (tbl.getParameters().containsKey(HiveKuduConstants.BUCKETS_PREFIX + partitionColumn)) {
          numberBuckets = Integer
              .valueOf(tbl.getParameters().get(HiveKuduConstants.BUCKETS_PREFIX + partitionColumn));
        } else {
          numberBuckets = HiveKuduConstants.DEFAULT_NUM_BUCKETS;
        }
        createTableOptions.addHashPartitions(Collections.singletonList(partitionColumn), numberBuckets);
      }

      client.createTable(kuduTableName, schema, createTableOptions);

    } catch (KuduException | SerDeException se) {
      se.printStackTrace();
      throw new MetaException("Error creating Kudu table: " + kuduTableName + ":" + se);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void commitCreateTable(Table tbl) throws MetaException {

    KuduClient client = HiveKuduBridgeUtils
        .getKuduClient(conf, tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
    try {
      if (!client.tableExists(getKuduTableName(tbl))) {
        throw new MetaException("table did not exist after trying to create it.");
      }
    } catch (KuduException e) {
      throw new MetaException("KuduException while checking if newly created table exists " + e);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void preDropTable(Table tbl) throws MetaException {
    // Nothing to do

  }

  @Override
  public void commitDropTable(Table tbl, boolean deleteData)
      throws MetaException {
    KuduClient client = HiveKuduBridgeUtils
        .getKuduClient(conf, tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
    String tablename = getKuduTableName(tbl);
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
    try {
      if (deleteData && !isExternal) {
        client.deleteTable(tablename);
      }
    } catch (Exception ioe) {
      throw new MetaException("Error dropping table:" + tablename);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void rollbackCreateTable(Table tbl) throws MetaException {
    KuduClient client = HiveKuduBridgeUtils
        .getKuduClient(conf, tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
    String tablename = getKuduTableName(tbl);
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
    try {
      if (client.tableExists(tablename) && !isExternal) {
        client.deleteTable(tablename);
      }
    } catch (Exception ioe) {
      throw new MetaException("Error dropping table while rollback of create table:" + tablename);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void rollbackDropTable(Table tbl) throws MetaException {
    // Nothing to do
  }

}
