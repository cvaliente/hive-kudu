package com.trivago.hive.serde.kudu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

@SuppressWarnings("WeakerAccess")
public class PartialRowWritable implements Writable {
  private PartialRow row;
  private Schema schema;


  public PartialRowWritable(Schema schema, PartialRow row) {
    this.schema = schema;
    this.row = row;
  }

  public PartialRow getRow() {
    return row;
  }

  // for reading the first value in the Kudu RecordReaderWrapper.createValue()
  public PartialRowWritable(RowResult rowResult) throws IOException {
    this.schema = rowResult.getSchema();
    this.row = schema.newPartialRow();
    HiveKuduBridgeUtils.copyRowResultToPartialRow(rowResult, this.row, this.schema);
  }

  // for updating the value in the Kudu RecordReaderWrapper.next()
  public void setRow(RowResult rowResult) throws IOException {
    this.schema = rowResult.getSchema();
    this.row = schema.newPartialRow();
    HiveKuduBridgeUtils.copyRowResultToPartialRow(rowResult, this.row, this.schema);
  }

  // Writing our RartialRow into the PartialRow of the Operation that will be applied by the KuduRecordUpdater
  public void mergeInto(PartialRow out) throws IOException {
    HiveKuduBridgeUtils.copyPartialRowToPartialRow(this.row, out, this.schema);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object obj = HiveKuduBridgeUtils.readObject(in, columnSchema.getType());
      HiveKuduBridgeUtils.setPartialRowValue(this.row, columnSchema, obj);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object obj = HiveKuduBridgeUtils.getRowValue(this.row, columnSchema);
      HiveKuduBridgeUtils.writeObject(obj, columnSchema.getType(), out);
    }
  }
}
