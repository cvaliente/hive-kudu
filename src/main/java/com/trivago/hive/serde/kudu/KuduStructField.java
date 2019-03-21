package com.trivago.hive.serde.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.kudu.ColumnSchema;

public class KuduStructField implements StructField {

  private ColumnSchema columnSchema;
  private int fieldId;
  private ObjectInspector fieldOi;

  @SuppressWarnings("WeakerAccess")
  public KuduStructField(ColumnSchema columnSchema, int fieldId) throws SerDeException {
    this.columnSchema = columnSchema;
    this.fieldId = fieldId;
    this.fieldOi = HiveKuduBridgeUtils.getObjectInspector(columnSchema);
  }

  @Override
  public String getFieldName() {
    return columnSchema.getName();
  }

  @Override
  public ObjectInspector getFieldObjectInspector() {
    return fieldOi;
  }

  @Override
  public int getFieldID() {
    return fieldId;
  }

  @Override
  public String getFieldComment() {
    return StringUtils.EMPTY;
  }

  @SuppressWarnings("WeakerAccess")
  public ColumnSchema getColumnSchema() {
    return columnSchema;
  }
}
