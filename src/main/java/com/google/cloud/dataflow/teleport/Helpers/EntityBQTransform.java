package com.google.cloud.dataflow.teleport.Helpers;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.Value;

import com.google.datastore.v1.Value.ValueTypeCase;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.Transport;

/**
 * Handles the Datastore Entity to BQ Row Transformation
 */
@AutoValue
public abstract class EntityBQTransform {
  abstract List<TableFieldSchema> rowSchema();
  abstract boolean strictCast();


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

  public TableRow toTableRow(Entity e) throws IOException {
    return toTableRow(rowSchema(), e);
  }

  private TableRow toTableRow(List<TableFieldSchema> tableSchema, Entity e) throws IOException {

    TableRow row = new TableRow();
    Map<String, Value> fields = e.getPropertiesMap();

    for (TableFieldSchema columnSchema : tableSchema) {
      if (columnSchema.getName().equals("__key__") && columnSchema.getType().toUpperCase().equals("STRING")) {
        row.put(columnSchema.getName(), keyToString(e.getKey()));
      }
      else if (fields.containsKey(columnSchema.getName())) {
        Value v = fields.get(columnSchema.getName());
        row.put(columnSchema.getName(), toRowValue(columnSchema, v));
      }
    }
    return row;
  }

  @Nullable
  private Object toRowValue(TableFieldSchema schema, Value v)
      throws IllegalArgumentException, IOException {
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

    return Timestamps.toString(v.getTimestampValue());
  }

  @Nullable
  private List<Object> valueToRepeated(TableFieldSchema schema, Value arrayValue)
      throws IOException {
    boolean isArrayValue = arrayValue.getValueTypeCase().equals(ValueTypeCase.ARRAY_VALUE);
    if (strictCast() && !isArrayValue) {
      return null;
    }

    ArrayList<Object> output = new ArrayList<>();
    TableFieldSchema repeatedSchema = schema.clone().setMode(null);

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
      Object rv = toRowValue(repeatedSchema, value);
      if (rv != null) {
        output.add(rv);
      }
    }
    return output;
  }

  @Nullable
  private TableRow valueToRecord(TableFieldSchema schema, Value v) throws IOException {
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
    if (strictCast() && !v.getValueTypeCase().equals(ValueTypeCase.DOUBLE_VALUE)) {
      return null;
    }

    switch (v.getValueTypeCase()) {
      case DOUBLE_VALUE:
        return v.getDoubleValue();
      case INTEGER_VALUE:
        return (double) v.getIntegerValue();
      case STRING_VALUE:
        if (Strings.isNullOrEmpty(v.getStringValue())) {
          return null;
        }

        try {
          return Double.parseDouble(v.getStringValue());
        } catch (NumberFormatException e) {}

        try {
          return ((Long) Long.parseLong(v.getStringValue())).doubleValue();
        } catch (NumberFormatException e) {}
    }
    return null;
  }

  @Nullable
  private Long valueToInt64(Value v) {
    if (strictCast() && !v.getValueTypeCase().equals(ValueTypeCase.INTEGER_VALUE)) {
      return null;
    }

    switch (v.getValueTypeCase()) {
      case INTEGER_VALUE:
        return v.getIntegerValue();
      case DOUBLE_VALUE:
        return Double.valueOf(v.getDoubleValue()).longValue();
      case STRING_VALUE:
        if (Strings.isNullOrEmpty(v.getStringValue())) {
          return null;
        }

        try {
          return Long.parseLong(v.getStringValue());
        } catch (NumberFormatException e) {}

        try {
          return ((Double) Double.parseDouble(v.getStringValue())).longValue();
        } catch (NumberFormatException e) {}
    }
    return null;
  }

  @Nullable
  private String valueToBytes(Value v) {
    if (v.getValueTypeCase().equals(ValueTypeCase.BLOB_VALUE)) {
      return Base64.getEncoder().encodeToString(v.getBlobValue().toByteArray());
    }
    return null;
  }

  /**
   * Converts a geo point to RFC 5870
   * @param v a Datastore Value
   * @return an RFC 5870 string encoded geopoint URI
   */
  @Nullable
  private String valueToGeopoint(Value v) {
    if (v.getGeoPointValue() == null || !v.getGeoPointValue().isInitialized()) {
      return null;
    }

    LatLng gp = v.getGeoPointValue();
    return String.format("geo:%s,%s",
        Double.toString(gp.getLatitude()),
        Double.toString(gp.getLongitude()));
  }

  @Nullable
  private String valueToString(Value v) throws IOException {
    if (strictCast() && !v.getValueTypeCase().equals(ValueTypeCase.STRING_VALUE)) {
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
        return valueToTimestamp(v);
      case NULL_VALUE:
        return null;
      case BLOB_VALUE:
        return valueToBytes(v);
      case ARRAY_VALUE:
        ArrayList<String> arr = new ArrayList<>();
        for (Value av: v.getArrayValue().getValuesList()) {
          arr.add(valueToString(av));
        }
        return Transport.getJsonFactory().toString(arr);
      case ENTITY_VALUE:
        try {
          return JsonFormat.printer()
              .usingTypeRegistry(TypeRegistry.newBuilder()
                  .add(Entity.getDescriptor())
                  .build())
              .omittingInsignificantWhitespace()
              .print(v.getEntityValue());
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
          return null;
        }
      case GEO_POINT_VALUE:
        return valueToGeopoint(v);
      case KEY_VALUE:
        return keyToString(v.getKeyValue());
      default:
        throw new IllegalArgumentException(
            "ValueType Case not handled: " + v.getValueTypeCase());
    }
  }

}
