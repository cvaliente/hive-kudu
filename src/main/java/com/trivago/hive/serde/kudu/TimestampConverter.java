package com.trivago.hive.serde.kudu;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

class TimestampConverter {

  static Long toKudu(TimestampWritable hiveTimestamp) {
    return TimeUnit.MILLISECONDS.toMicros(hiveTimestamp.getTimestamp().getTime());
  }

  static TimestampWritable toHive(Long kuduTimestamp) {
    return new TimestampWritable(new Timestamp(TimeUnit.MICROSECONDS.toMillis(kuduTimestamp)));
  }
}
