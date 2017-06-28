package com.google.cloud.dataflow.teleport.Helpers;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.teleport.Helpers.AutoValue_EntityBQTransform.Builder;
import com.google.common.base.Strings;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.Value;

import com.google.datastore.v1.Value.ValueTypeCase;
import com.google.gson.Gson;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Handles the Datastore Entity to BQ Row Transformation
 */
@AutoValue
public abstract class EntityBQTransform {
  abstract List<TableFieldSchema> getRowSchema();
  abstract boolean getStrictCast();


  @AutoValue.Builder
  public abstract static class Builder {
    public abstract EntityBQTransform.Builder setRowSchema(List<TableFieldSchema> rowSchema);
    public abstract EntityBQTransform.Builder setStrictCast(boolean strictCast);
    public abstract EntityBQTransform build();
  }

  public static Builder newBuilder() {
    return new com.google.cloud.dataflow.teleport.Helpers.AutoValue_EntityBQTransform.Builder()
        .setStrictCast(false);
  }

  public TableRow toTableRow(Entity e) {
    return toTableRow(getRowSchema(), e);
  }

  private TableRow toTableRow(List<TableFieldSchema> tableSchema, Entity e) {

    TableRow row = new TableRow();
    Map<String, Value> fields = e.getPropertiesMap();

    for (TableFieldSchema columnSchema : tableSchema) {
      if (columnSchema.getName().equals("__key__") && columnSchema.getType().toUpperCase().equals("STRING")) {
        row.put(columnSchema.getName(), keyToString(e.getKey()));
      }
      else if (fields.containsKey(columnSchema.getName())) {
        Value v = fields.get(columnSchema.getName());
        row.put(columnSchema.getName(), toRowValue(columnSchema, v));
      } else {
        System.err.println("Do not have field in entity: " + columnSchema.getName());
      }
    }
    return row;
  }

  @Nullable
  private Object toRowValue(TableFieldSchema schema, Value v) throws IllegalArgumentException {
    // handle repeated rows
    if (!Strings.isNullOrEmpty(schema.getMode()) && schema.getMode().toUpperCase().equals("REPEATED")) {
      return valueToRepeated(schema, v);
    }

    // Handle if not repeated
    switch (schema.getType().toUpperCase()) {
      case "STRING":
        return valueToString(v);
      case "BYTES":
        return valueToBytes(v);
      case "INTEGER":
        return valueToInt64(v);
      case "FLOAT":
        return valueToFloat64(v);
      case "BOOLEAN":
        return valueToBoolean(v);
      case "RECORD":
        return valueToRecord(schema, v);
      case "TIMESTAMP":
        return valueToTimestamp(v);
      case "DATE":
        return valueToDate(v);
      case "TIME":
        return valueToTime(v);
      case "DATETIME":
        return valueToDateTime(v);
      default:
        throw new IllegalArgumentException(
            "Unknown TableFieldSchema Type of: " + schema.getType().toUpperCase());
    }
  }

  @Nullable
  private String keyToString(Key k) {
    StringBuilder sb = new StringBuilder();

    List<String> paths = new ArrayList<>();
    for (PathElement p : k.getPathList()) {
      if (!Strings.isNullOrEmpty(p.getName())) {
        paths.add(String.format("%s, \'%s\'", p.getKind(), p.getName().replace("'", "\'")));
      } else {
        paths.add(String.format("%s, %s", p.getKind(), Long.toString(p.getId())));
      }
    }

    return "key(" + String.join(", ", paths) + ")";
  }

  @Nullable
  private String valueToDateTime(Value v) {
    String date = valueToDate(v);
    String time = valueToTime(v);
    if (date == null || time == null) {
      return null;
    }
    return date + " " + time;
  }

  @Nullable
  private String valueToTime(Value v) {
    String timestamp = valueToTimestamp(v);
    if (timestamp == null) {
      return null;
    }
    return timestamp.split("T")[1].replace("Z","");
  }

  @Nullable
  private String valueToDate(Value v) {
    String timestamp = valueToTimestamp(v);
    if (timestamp == null) {
      return null;
    }
    return timestamp.split("T")[0];
  }

  @Nullable
  private String valueToTimestamp(Value v) {
    boolean isTimestampValue = v.getValueTypeCase().equals(ValueTypeCase.TIMESTAMP_VALUE);
    if (!isTimestampValue) {
      return null;
    }

    long seconds = v.getTimestampValue().getSeconds();
    long millis = TimeUnit.MILLISECONDS.convert(
        v.getTimestampValue().getNanos(), TimeUnit.NANOSECONDS);

    return Instant.ofEpochMilli(seconds * 1000 + millis).toString();
  }

  @Nullable
  private List<Object> valueToRepeated(TableFieldSchema schema, Value arrayValue) {
    boolean isArrayValue = arrayValue.getValueTypeCase().equals(ValueTypeCase.ARRAY_VALUE);
    if (getStrictCast() && !isArrayValue) {
      return null;
    }

    ArrayList<Object> output = new ArrayList<>();
    TableFieldSchema repeatedSchema = schema.clone().setMode(null);
//    TableFieldSchema repeatedSchema = new TableFieldSchema()
//        .setName(schema.getName())
//        .setDescription(schema.getDescription())
//        .setType(schema.getType())
//        .setFields(schema.getFields());


    // Handle the non strict cast requirement of having a single entity, even though
    // bq schema says its a repeated field
    List<Value> arrayValues;
    if (isArrayValue) {
      arrayValues = arrayValue.getArrayValue().getValuesList();
    } else {
      arrayValues = new ArrayList<>();
      arrayValues.add(arrayValue);
    }

    for (Value value : arrayValues) {
      output.add(toRowValue(repeatedSchema, value));
    }
    return output;
  }

  @Nullable
  private TableRow valueToRecord(TableFieldSchema schema, Value v) {
    boolean isEntityValue = v.getValueTypeCase().equals(ValueTypeCase.ENTITY_VALUE);
    if (!isEntityValue) {
      return null;
    }

    Entity entity = v.getEntityValue();
    return toTableRow(schema.getFields(), entity);
  }

  @Nullable
  private Boolean valueToBoolean(Value v) {
    if (v.getValueTypeCase().equals(ValueTypeCase.BOOLEAN_VALUE)) {
      return v.getBooleanValue();
    } else {
      return null;
    }
  }

  @Nullable
  private Double valueToFloat64(Value v) {
    if (getStrictCast() && !v.getValueTypeCase().equals(ValueTypeCase.DOUBLE_VALUE)) {
      return null;
    }

    switch (v.getValueTypeCase()) {
      case DOUBLE_VALUE:
        return v.getDoubleValue();
      case INTEGER_VALUE:
        return (double) v.getIntegerValue();
    }
    return null;
  }

  @Nullable
  private Long valueToInt64(Value v) {
    if (getStrictCast() && !v.getValueTypeCase().equals(ValueTypeCase.INTEGER_VALUE)) {
      return null;
    }

    switch (v.getValueTypeCase()) {
      case INTEGER_VALUE:
        return v.getIntegerValue();
      case DOUBLE_VALUE:
        return (long) v.getDoubleValue();
    }
    return null;
  }

  @Nullable
  private byte[] valueToBytes(Value v) {
    if (!v.getValueTypeCase().equals(ValueTypeCase.BLOB_VALUE)) {
      return v.getBlobValue().toByteArray();
    }

    return null;
  }

  @Nullable
  private String valueToString(Value v) {
    if (getStrictCast() && !v.getValueTypeCase().equals(ValueTypeCase.STRING_VALUE)) {
      return null;
    }

    switch (v.getValueTypeCase()) {
      case STRING_VALUE:
        return v.getStringValue();
      case INTEGER_VALUE:
        return Long.toString(v.getIntegerValue());
      case DOUBLE_VALUE:
        return Double.toString(v.getDoubleValue());
      case BOOLEAN_VALUE:
        return Boolean.toString(v.getBooleanValue());
      case TIMESTAMP_VALUE:
        // to RFC 3339 date string format
        return v.getTimestampValue().toString();
      case NULL_VALUE:
        return null;
      case BLOB_VALUE:
        return Base64.getEncoder().encodeToString(v.getBlobValue().toByteArray());
      case ARRAY_VALUE:
        ArrayList<String> arr = new ArrayList<>();
        for (Value arrV : v.getArrayValue().getValuesList()) {
          arr.add(valueToString(v));
        }
        return new Gson().toJson(arr);
      case ENTITY_VALUE:
        return new Gson().toJson(v.getEntityValue());
      case GEO_POINT_VALUE:
        return new Gson().toJson(v.getGeoPointValue());
      case KEY_VALUE:
        return new Gson().toJson(v.getKeyValue());
      default:
        throw new IllegalArgumentException(
            "ValueType Case not handled: " + v.getValueTypeCase());
    }
  }


}
