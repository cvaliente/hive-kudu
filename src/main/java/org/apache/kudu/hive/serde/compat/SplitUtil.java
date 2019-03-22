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

package org.apache.kudu.hive.serde.compat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/*
Copied over from com.twitter.elephantbird.util.SplitUtil which in turn copied it from Apache Pig.
We'd like to avoid importing those as dependencies for just a few helper methods.
 */

public class SplitUtil {

  public static void serializeInputSplit(Configuration conf, DataOutputStream out, InputSplit split)
      throws IOException {
    Class<? extends InputSplit> clazz = split.getClass().asSubclass(InputSplit.class);
    Text.writeString(out, clazz.getName());
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = factory.getSerializer(clazz);
    serializer.open(out instanceof UncloseableDataOutputStream ? out : new UncloseableDataOutputStream(out));
    //noinspection unchecked
    serializer.serialize(split);
  }

  public static InputSplit deserializeInputSplit(Configuration conf, DataInputStream in) throws IOException {
    String name = Text.readString(in);
    Class<? extends InputSplit> clazz;
    try {
      clazz = conf.getClassByName(name).asSubclass(InputSplit.class);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not find class for deserialized class name: " + name, e);
    }
    return deserializeInputSplitInternal(
        conf, in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in), clazz);
  }

  private static <T extends InputSplit> T deserializeInputSplitInternal(
      Configuration conf, DataInputStream in, Class<T> clazz) throws IOException {
    T split = ReflectionUtils.newInstance(clazz, conf);
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer<T> deserializer = factory.getDeserializer(clazz);
    deserializer.open(in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in));
    return deserializer.deserialize(split);
  }

  private static class UncloseableDataOutputStream extends DataOutputStream {

    UncloseableDataOutputStream(DataOutputStream os) {
      super(os);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }

  private static class UncloseableDataInputStream extends DataInputStream {

    UncloseableDataInputStream(DataInputStream is) {
      super(is);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }
}
