package com.trivago.hive.serde.kudu.output;

import com.trivago.hive.serde.kudu.PartialRowWritable;
import com.trivago.hive.serde.kudu.compat.HadoopCompat;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.kudu.mapreduce.KuduTableOutputFormat;

public class HiveKuduOutputFormat implements AcidOutputFormat<NullWritable, PartialRowWritable>, Configurable {

  private KuduTableOutputFormat kuduTableOutputFormat = new KuduTableOutputFormat();

  @Override
  public Configuration getConf() {
    return kuduTableOutputFormat.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    kuduTableOutputFormat.setConf(conf);
  }

  @Override
  public RecordUpdater getRecordUpdater(Path path, Options options) throws IOException {
    return getKuduRecordUpdater(options.getConfiguration(), options.getReporter());
  }

  @Override
  public RecordWriter getRawRecordWriter(Path path, Options options) throws IOException {
    return getKuduRecordUpdater(options.getConfiguration(), options.getReporter());
  }

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    return getKuduRecordUpdater(jc, progress);
  }

  private KuduRecordUpdater getKuduRecordUpdater(Configuration jc, Progressable progress) throws IOException {
    return new KuduRecordUpdater(kuduTableOutputFormat, jc, progress);
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, PartialRowWritable> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    throw new RuntimeException("getRecordWriter should not be called on a HiveOutputFormat, something went terribly wrong");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    try {
      kuduTableOutputFormat.checkOutputSpecs(HadoopCompat.newJobContext(job, null));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
