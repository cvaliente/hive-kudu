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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.kudu.hive.serde.compat.SplitUtil;

public class InputSplitWrapper implements InputSplit, Configurable {

  private org.apache.hadoop.mapreduce.InputSplit realSplit;
  private Configuration conf;


  @SuppressWarnings("unused") // MapReduce instantiates this.
  public InputSplitWrapper() {
  }

  InputSplitWrapper(org.apache.hadoop.mapreduce.InputSplit realSplit) {
    this.realSplit = realSplit;
  }

  @Override
  public long getLength() throws IOException {
    try {
      return realSplit.getLength();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String[] getLocations() throws IOException {
    try {
      return realSplit.getLocations();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (!(in instanceof DataInputStream)) {
      throw new IOException("can only read from DataInputStream");
    }
    realSplit = SplitUtil.deserializeInputSplit(conf, (DataInputStream) in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (!(out instanceof DataOutputStream)) {
      throw new IOException("can only read from DataOutputStream");
    }
    SplitUtil.serializeInputSplit(conf, (DataOutputStream) out, realSplit);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public org.apache.hadoop.mapreduce.InputSplit getRealSplit() {
    return realSplit;
  }


}
