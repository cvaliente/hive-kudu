package com.trivago.hive.udf.utils.objectinspectorutils.inpectors;

import java.util.*;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class RenameStructFieldsStructObjectInspector extends
        StructObjectInspector {


  private Map<String, RenamedStructField> myFields = new HashMap<>();
  private StructObjectInspector oi;

  public RenameStructFieldsStructObjectInspector(Map<String, String> fromTo,
                                                 StructObjectInspector parent) throws SerDeException {
    this.oi = parent;
    List<String> fields = new LinkedList<>(fromTo.values());

    for (StructField var : ((StructObjectInspector) parent).getAllStructFieldRefs()) {

      RenamedStructField renamedField = new RenamedStructField();
      renamedField.theirs = var;

      if (fromTo.containsKey(var.getFieldName())) {
        renamedField.ourName = fromTo.get(var.getFieldName());
        myFields.put(fromTo.get(var.getFieldName()), renamedField);
      }
      else if (fields.contains(var.getFieldName())) {
        renamedField.ourName = var.getFieldName();
        myFields.put(var.getFieldName(), renamedField);
      }
      else {
        StringBuilder blob = new StringBuilder();
        blob.append("Could not find field ").append(var.getFieldName()).append(". + \n");
        blob.append("The original column names: [");
        for (String field : fromTo.values()) {
          blob.append(field).append(", \n");
        }

        blob.append("] \n The output column names: [");
        for (String field : fromTo.keySet()) {
          blob.append(field).append(", \n");
        }

        throw new SerDeException(blob.toString() + "].");
      }
    }
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return new LinkedList<StructField>(myFields.values());
  }

  @Override
  public Object getStructFieldData(Object arg0, StructField arg1) {
    RenamedStructField renamedField = (RenamedStructField) arg1;
    return this.oi.getStructFieldData(arg0, renamedField.theirs);
  }

  @Override
  public StructField getStructFieldRef(String arg0) {
    return myFields.get(arg0);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object arg0) {
    ArrayList<Object> ret = new ArrayList<>(myFields.size());
    for (RenamedStructField renamed : myFields.values()) {
      ret.add(this.oi.getStructFieldData(arg0, renamed.theirs));
    }
    return ret;
  }

  private static class RenamedStructField implements StructField {

    private StructField theirs;
    private String ourName;

    @Override
    public String getFieldName() {
      return ourName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return theirs.getFieldObjectInspector();
    }

    @Override
    public int getFieldID() {
      return theirs.getFieldID();
    }

    @Override
    public String getFieldComment() {
      return theirs.getFieldComment();
    }

  }

}
