package com.trivago.hive.serde.kudu.input;

import com.trivago.hive.serde.kudu.compat.SplitUtil;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

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
