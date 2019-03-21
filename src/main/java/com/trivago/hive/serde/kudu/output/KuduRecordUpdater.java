package com.trivago.hive.serde.kudu.output;

import com.trivago.hive.serde.kudu.HiveKuduBridgeUtils;
import com.trivago.hive.serde.kudu.HiveKuduConstants;
import com.trivago.hive.serde.kudu.PartialRowWritable;
import com.trivago.hive.serde.kudu.compat.HadoopCompat;
import com.trivago.hive.serde.kudu.compat.ReporterWrapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduRecordUpdater implements RecordWriter, RecordUpdater {


  private static final Logger LOGGER = LoggerFactory.getLogger(KuduRecordUpdater.class);
  private org.apache.hadoop.mapreduce.RecordWriter<NullWritable, Operation> realWriter;
  private KuduTable table;
  private KuduClient client;

  @SuppressWarnings("unchecked")
  KuduRecordUpdater(OutputFormat<NullWritable, Operation> realOutputFormat,
      Configuration jobConf, Progressable progress)
      throws IOException {

    this.client = HiveKuduBridgeUtils.getKuduClient(jobConf);

    String tableName = jobConf.get(HiveKuduConstants.OUTPUT_TABLE_KEY);
    try {
      this.table = this.client.openTable(tableName);
    } catch (KuduException e) {
      throw new RuntimeException(this.getClass().getName() +
          " could not obtain the table from the master, is the master running and is this table created? tablename="
              + tableName, e);
    }

    try {
      // create a MapContext to provide access to the reporter (for counters)
      TaskAttemptContext taskContext = HadoopCompat.newMapContext(
          jobConf, TaskAttemptID.forName(jobConf.get(IOConstants.MAPRED_TASK_ID)),
          null, null, null, new ReporterWrapper((Reporter) progress), null);

      realWriter = realOutputFormat.getRecordWriter(taskContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  @Override
  public void write(Writable row) throws IOException {
    this.apply(table.newUpsert(), row);
  }

  @Override
  public void insert(long currentTransaction, Object row) throws IOException {
    this.apply(table.newInsert(), row);
  }

  @Override
  public void update(long currentTransaction, Object row) throws IOException {
    this.apply(table.newUpdate(), row);
  }

  @Override
  public void delete(long currentTransaction, Object row) throws IOException {
    this.apply(table.newDelete(), row);
  }

  private void apply(Operation operation, Object row) throws IOException{

    if(!(row instanceof PartialRowWritable))
      throw new IOException("Only accepts PartialRowWritable as Input");
    PartialRowWritable writable = (PartialRowWritable) row;

    try {
      writable.mergeInto(operation.getRow());
      realWriter.write(null, operation);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public void flush() throws IOException {
  }

  @Override
  public void close(boolean abort) throws IOException {
    try {
      LOGGER.info("closing client, statistics");
      LOGGER.info(client.getStatistics().toString());
      client.close();
      realWriter.close(null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public SerDeStats getStats() {
    return HiveKuduBridgeUtils.convertStatistics(client.getStatistics());
  }
}
