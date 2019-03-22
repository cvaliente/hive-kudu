// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.hive.serde.input;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kudu.hive.serde.PartialRowWritable;
import org.apache.kudu.hive.serde.compat.HadoopCompat;
import org.apache.kudu.mapreduce.KuduTableInputFormat;

public class KuduTableInputFormatWrapper implements org.apache.hadoop.mapred.InputFormat, Configurable {

  private KuduTableInputFormat kuduTableInputFormat = new KuduTableInputFormat();

  @Override
  public Configuration getConf() {
    return kuduTableInputFormat.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    kuduTableInputFormat.setConf(conf);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {
      List<org.apache.hadoop.mapreduce.InputSplit> splits =
          kuduTableInputFormat.getSplits(HadoopCompat.newJobContext(job, null));

      if (splits == null) {
        return new InputSplit[0];
      }

      InputSplit[] resultSplits = new InputSplit[splits.size()];
      int i = 0;
      for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
        InputSplitWrapper wrapper = new InputSplitWrapper(split);
        wrapper.setConf(job);
        resultSplits[i++] = wrapper;
      }

      return resultSplits;

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader<NullWritable, PartialRowWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    return new RecordReaderWrapper(kuduTableInputFormat, split, job, reporter);
  }

}
