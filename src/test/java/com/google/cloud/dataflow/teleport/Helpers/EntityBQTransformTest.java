package com.google.cloud.dataflow.teleport.Helpers;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.PartitionId;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.crypto.dsig.keyinfo.KeyValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 6/20/17.
 */
public class EntityBQTransformTest {

  private TableSchema exampleTable() {
    TableSchema personTable = new TableSchema();
    personTable.setFields(Arrays.asList(
        new TableFieldSchema().setName("__key__").setType("STRING"),
        new TableFieldSchema().setName("firstName").setType("STRING"),
        new TableFieldSchema().setName("age").setType("INTEGER"),
        new TableFieldSchema().setName("pic").setType("BYTES"),
        new TableFieldSchema().setName("weight").setType("FLOAT"),
        new TableFieldSchema().setName("isGoogler").setType("BOOLEAN"),
        new TableFieldSchema().setName("birthTimestamp").setType("TIMESTAMP"),
        new TableFieldSchema().setName("birthDate").setType("DATE"),
        new TableFieldSchema().setName("birthTime").setType("TIME"),
        new TableFieldSchema().setName("birthDateTime").setType("DATETIME"),
        new TableFieldSchema().setName("favFruits").setType("STRING").setMode("REPEATED"),
        new TableFieldSchema().setName("body").setType("RECORD").setFields(Arrays.asList(
            new TableFieldSchema().setName("height").setType("FLOAT"),
            new TableFieldSchema().setName("ethnicity").setType("STRING")
        )),
        new TableFieldSchema().setName("vacations").setType("RECORD").setMode("REPEATED")
            .setFields(Arrays.asList(
                new TableFieldSchema().setName("place").setType("STRING"),
                new TableFieldSchema().setName("time").setType("TIMESTAMP")
            ))
    ));
    return personTable;
  }

  @Test
  public void testEntityBQTransform_toTableRow() throws IOException {
    Entity e;
    TableRow r;
    EntityBQTransform ebt;
    e = Entity.newBuilder()
        .setKey(Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("my-awesome-project"))
            .addPath(PathElement.newBuilder()
                .setKind("SomeKind")
                .setName("myKey")))
        .putProperties("firstName", Value.newBuilder()
            .setStringValue("Colin").build())
        .putProperties("age", Value.newBuilder()
            .setIntegerValue(25).build())
        // Weight is purposly set as a string to test strict v non strict cast
        .putProperties("weight", Value.newBuilder()
            .setStringValue("195").build())
        .putProperties("vacations", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
            .addValues(Value.newBuilder().setEntityValue(Entity.newBuilder()
                .putProperties("place", Value.newBuilder().setStringValue("Atlanta").build())
                .putProperties("time", Value.newBuilder().setTimestampValue(
                    Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
            ))
            .addValues(Value.newBuilder().setEntityValue(Entity.newBuilder()
                .putProperties("place", Value.newBuilder().setStringValue("New York").build())
                .putProperties("time", Value.newBuilder().setTimestampValue(
                    Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
            ))).build())
        .build();

    // handle strict case first
    ebt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    r = ebt.toTableRow(e);
    Assert.assertEquals("Colin", r.get("firstName"));
    Assert.assertEquals(25L, r.get("age"));
    Assert.assertEquals("key(SomeKind, 'myKey')", r.get("__key__"));
    Assert.assertEquals(null, r.get("weight"));

    // handle non strict casting
    ebt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();
    r = ebt.toTableRow(e);
    Assert.assertEquals("Colin", r.get("firstName"));
    Assert.assertEquals(25L, r.get("age"));
    Assert.assertEquals("key(SomeKind, 'myKey')", r.get("__key__"));
    Assert.assertEquals(195.0, r.get("weight"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_string() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    // Test for a real strict
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setStringValue("colin").build())
        .build();
    Assert.assertEquals("colin", strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("colin", nonstrictEbt.toTableRow(e).get("firstName"));

    // Test Integer Casting
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setIntegerValue(1234L).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("1234", nonstrictEbt.toTableRow(e).get("firstName"));

    // Test Double Casting
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setDoubleValue(1234.4321).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("1234.4321", nonstrictEbt.toTableRow(e).get("firstName"));

    // Test BOOLEAN_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setBooleanValue(true).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("true", nonstrictEbt.toTableRow(e).get("firstName"));

    // Test TIMESTAMP_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setTimestampValue(
            Timestamp.newBuilder().setSeconds(1498781804).setNanos(12345)).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("2017-06-30T00:16:44.000012345Z",
        nonstrictEbt.toTableRow(e).get("firstName"));

    // Test NULL_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals(null, nonstrictEbt.toTableRow(e).get("firstName"));

    // BLOB_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setBlobValue(
            ByteString.copyFromUtf8("HI WORLD")).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("SEkgV09STEQ=", nonstrictEbt.toTableRow(e).get("firstName"));

    // ARRAY_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue("My String"))
                .addValues(Value.newBuilder().setStringValue("My Other String")).build())
            .build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("[\"My String\",\"My Other String\"]",
        nonstrictEbt.toTableRow(e).get("firstName"));

    // GEO_POINT_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setGeoPointValue(
            LatLng.newBuilder().setLatitude(100).setLongitude(-100)).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("geo:100.0,-100.0", nonstrictEbt.toTableRow(e).get("firstName"));

    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setGeoPointValue(
            LatLng.newBuilder().setLatitude(-1234.567901).setLongitude(1234.56900)).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("geo:-1234.567901,1234.569", nonstrictEbt.toTableRow(e).get("firstName"));

    // KEY_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setKeyValue(
            Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                    .setProjectId("my-awesome-project"))
                .addPath(PathElement.newBuilder()
                    .setKind("SomeKind")
                    .setName("myKey"))
                .addPath(PathElement.newBuilder()
                    .setKind("SomeOtherKind")
                    .setId(11990011L))).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("key(SomeKind, 'myKey', SomeOtherKind, 11990011)",
        nonstrictEbt.toTableRow(e).get("firstName"));

    // ENTITY_VALUE
    e = Entity.newBuilder()
        .putProperties("firstName", Value.newBuilder().setEntityValue(Entity.newBuilder()
            .putProperties("someProp", Value.newBuilder().setStringValue("Some String").build())
            .putProperties("someProp2", Value.newBuilder().setBooleanValue(true).build())
            .putProperties("someProp3", Value.newBuilder().setDoubleValue(1234.1234).build())
        ).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("firstName"));
    Assert.assertEquals("{\"properties\":{\"someProp\":{\"stringValue\":\"Some String\"},\"someProp2\":{\"booleanValue\":true},\"someProp3\":{\"doubleValue\":1234.1234}}}", nonstrictEbt.toTableRow(e).get("firstName"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_bytes() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    // Test for a real strict
    e = Entity.newBuilder()
        .putProperties("pic", Value.newBuilder().setBlobValue(
            ByteString.copyFromUtf8("HI WORLD")).build())
        .build();

    Assert.assertEquals("SEkgV09STEQ=", strictEbt.toTableRow(e).get("pic"));
    Assert.assertEquals("SEkgV09STEQ=", nonstrictEbt.toTableRow(e).get("pic"));

    // Test lazy case
    e = Entity.newBuilder()
        .putProperties("pic", Value.newBuilder().setStringValue("MY STRING").build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("pic"));
    Assert.assertEquals(null, nonstrictEbt.toTableRow(e).get("pic"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_integer() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    // Test strict
    e = Entity.newBuilder()
        .putProperties("age", Value.newBuilder().setIntegerValue(35L).build())
        .build();
    Assert.assertEquals(35L, strictEbt.toTableRow(e).get("age"));
    Assert.assertEquals(35L, nonstrictEbt.toTableRow(e).get("age"));

    // Test nonstrict
    e = Entity.newBuilder()
        .putProperties("age", Value.newBuilder().setDoubleValue(35.8).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("age"));
    Assert.assertEquals(35L, nonstrictEbt.toTableRow(e).get("age"));

    e = Entity.newBuilder()
        .putProperties("age", Value.newBuilder().setStringValue("35").build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("age"));
    Assert.assertEquals(35L, nonstrictEbt.toTableRow(e).get("age"));

    e = Entity.newBuilder()
        .putProperties("age", Value.newBuilder().setStringValue("35.8").build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("age"));
    Assert.assertEquals(35L, nonstrictEbt.toTableRow(e).get("age"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_float() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    // Test strict
    e = Entity.newBuilder()
        .putProperties("weight", Value.newBuilder().setDoubleValue(150.51).build())
        .build();
    Assert.assertEquals(150.51, strictEbt.toTableRow(e).get("weight"));
    Assert.assertEquals(150.51, nonstrictEbt.toTableRow(e).get("weight"));

    // Test nonstrict
    e = Entity.newBuilder()
        .putProperties("weight", Value.newBuilder().setIntegerValue(150L).build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("weight"));
    Assert.assertEquals(150.0, nonstrictEbt.toTableRow(e).get("weight"));

    e = Entity.newBuilder()
        .putProperties("weight", Value.newBuilder().setStringValue("35").build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("weight"));
    Assert.assertEquals(35.0, nonstrictEbt.toTableRow(e).get("weight"));

    e = Entity.newBuilder()
        .putProperties("weight", Value.newBuilder().setStringValue("35.8").build())
        .build();
    Assert.assertEquals(null, strictEbt.toTableRow(e).get("weight"));
    Assert.assertEquals(35.8, nonstrictEbt.toTableRow(e).get("weight"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_boolean() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    // Test strict
    e = Entity.newBuilder()
        .putProperties("isGoogler", Value.newBuilder().setBooleanValue(true).build())
        .build();
    Assert.assertEquals(true, strictEbt.toTableRow(e).get("isGoogler"));
    Assert.assertEquals(true, nonstrictEbt.toTableRow(e).get("isGoogler"));

    e = Entity.newBuilder()
        .putProperties("isGoogler", Value.newBuilder().setBooleanValue(false).build())
        .build();
    Assert.assertEquals(false, strictEbt.toTableRow(e).get("isGoogler"));
    Assert.assertEquals(false, nonstrictEbt.toTableRow(e).get("isGoogler"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_record() throws IOException {
    Entity e;
    List<?> vacations;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    e = Entity.newBuilder()
        .putProperties("vacations", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
            .addValues(Value.newBuilder().setEntityValue(Entity.newBuilder()
                .putProperties("place", Value.newBuilder().setStringValue("Atlanta").build())
                .putProperties("time", Value.newBuilder().setTimestampValue(
                    Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
            ))
            .addValues(Value.newBuilder().setEntityValue(Entity.newBuilder()
                .putProperties("place", Value.newBuilder().setStringValue("New York").build())
                .putProperties("time", Value.newBuilder().setTimestampValue(
                    Timestamp.newBuilder().setNanos(5000).setSeconds(10).build()).build())
            ))
        ).build())
        .build();

    // Test Strict
    vacations = (List<?>) strictEbt.toTableRow(e).get("vacations");

    Assert.assertEquals(2, vacations.size());
    Assert.assertEquals(TableRow.class, vacations.get(0).getClass());
    Assert.assertEquals(TableRow.class, vacations.get(1).getClass());

    TableRow r1 = (TableRow) vacations.get(0);
    TableRow r2 = (TableRow) vacations.get(1);
    Assert.assertEquals("Atlanta", r1.get("place"));
    Assert.assertEquals("2017-06-28T23:28:20.000001Z", r1.get("time"));

    Assert.assertEquals("New York", r2.get("place"));
    Assert.assertEquals("1970-01-01T00:00:10.000005Z", r2.get("time"));

    e = Entity.newBuilder()
        .putProperties("vacations", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("HI WORLD")).build())
            .build())
        .build();

    vacations = (List<?>) strictEbt.toTableRow(e).get("vacations");
    Assert.assertEquals(0, vacations.size());

    vacations  = (List<?>) nonstrictEbt.toTableRow(e).get("vacations");
    Assert.assertEquals(0, vacations.size());
  }

  @Test
  public void testEntityBQTransform_toTableRow_timestamp() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    e = Entity.newBuilder()
        .putProperties("birthTimestamp", Value.newBuilder().setTimestampValue(
            Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
        .build();

    Assert.assertEquals("2017-06-28T23:28:20.000001Z", strictEbt.toTableRow(e).get("birthTimestamp"));
    Assert.assertEquals("2017-06-28T23:28:20.000001Z", nonstrictEbt.toTableRow(e).get("birthTimestamp"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_date() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    e = Entity.newBuilder()
        .putProperties("birthDate", Value.newBuilder().setTimestampValue(
            Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
        .build();

    Assert.assertEquals("2017-06-28", strictEbt.toTableRow(e).get("birthDate"));
    Assert.assertEquals("2017-06-28", nonstrictEbt.toTableRow(e).get("birthDate"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_time() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    e = Entity.newBuilder()
        .putProperties("birthTime", Value.newBuilder().setTimestampValue(
            Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
        .build();

    Assert.assertEquals("23:28:20.000001", strictEbt.toTableRow(e).get("birthTime"));
    Assert.assertEquals("23:28:20.000001", nonstrictEbt.toTableRow(e).get("birthTime"));
  }

  @Test
  public void testEntityBQTransform_toTableRow_datetime() throws IOException {
    Entity e;

    EntityBQTransform strictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    EntityBQTransform nonstrictEbt = EntityBQTransform.newBuilder()
        .setStrictCast(false)
        .setRowSchema(exampleTable().getFields())
        .build();

    e = Entity.newBuilder()
        .putProperties("birthDateTime", Value.newBuilder().setTimestampValue(
            Timestamp.newBuilder().setNanos(1000).setSeconds(1498692500).build()).build())
        .build();

    Assert.assertEquals("2017-06-28 23:28:20.000001", strictEbt.toTableRow(e).get("birthDateTime"));
    Assert.assertEquals("2017-06-28 23:28:20.000001", nonstrictEbt.toTableRow(e).get("birthDateTime"));
  }
}