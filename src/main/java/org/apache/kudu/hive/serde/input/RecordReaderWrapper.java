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

import org.apache.kudu.hive.serde.PartialRowWritable;
import org.apache.kudu.hive.serde.compat.HadoopCompat;
import org.apache.kudu.hive.serde.compat.ReporterWrapper;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.kudu.client.RowResult;

public class RecordReaderWrapper implements RecordReader<NullWritable, PartialRowWritable> {

  private org.apache.hadoop.mapreduce.RecordReader<NullWritable, RowResult> realReader;

  private long splitLen; // for getPos()

  private boolean firstRecord = false;
  private boolean eof = false;

  RecordReaderWrapper(InputFormat<NullWritable, RowResult> newInputFormat, InputSplit oldSplit,
      JobConf oldJobConf, Reporter reporter) throws IOException {
    splitLen = oldSplit.getLength();

    org.apache.hadoop.mapreduce.InputSplit split = ((InputSplitWrapper) oldSplit).getRealSplit();

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get("mapred.task.id"));
    if (taskAttemptID == null) {
      taskAttemptID = new TaskAttemptID();
    }

    // create a MapContext to pass reporter to record reader (for counters)
    TaskAttemptContext taskContext = HadoopCompat
        .newMapContext(oldJobConf, taskAttemptID, null, null, null,
            new ReporterWrapper(reporter), null);
    try {
      realReader = newInputFormat.createRecordReader(split, taskContext);
      realReader.initialize(split, taskContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean next(NullWritable key, PartialRowWritable value) throws IOException {
    if (eof) {
      return false;
    }

    if (firstRecord) { // key & value are already read.
      firstRecord = false;
      return true;
    }

    try {
      if (realReader.nextKeyValue()) {
        value.setRow(realReader.getCurrentValue());
        return true;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    eof = true; // strictly not required, just for consistency
    return false;
  }


  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public PartialRowWritable createValue() {
    PartialRowWritable value = null;
    try {
      if (!firstRecord && !eof) {
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          value = new PartialRowWritable(realReader.getCurrentValue());
        } else {
          eof = true;
        }
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not read first record (and it was not an EOF)", e);
    }
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return (long) (splitLen * getProgress());
  }

  @Override
  public void close() throws IOException {
    realReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    try {
      return realReader.getProgress();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
