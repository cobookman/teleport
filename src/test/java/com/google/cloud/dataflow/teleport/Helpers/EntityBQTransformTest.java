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
import com.google.gson.Gson;
import com.google.protobuf.Timestamp;
import java.util.Arrays;
import java.util.List;
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
        new TableFieldSchema().setName("birthTimeStamp").setType("TIMESTAMP"),
        new TableFieldSchema().setName("birthDate").setType("DATE"),
        new TableFieldSchema().setName("birthTime").setType("TIME"),
        new TableFieldSchema().setName("birthDateTime").setType("DATETIME"),
        new TableFieldSchema().setName("favFruits").setType("STRING").setMode("REPEATED"),
        new TableFieldSchema().setName("body").setType("RECORD").setFields(Arrays.asList(
            new TableFieldSchema().setName("height").setType("FLOAT"),
            new TableFieldSchema().setName("ethnicity").setType("STRING")
        )),
        new TableFieldSchema().setName("vacations").setType("RECORD").setMode("REPEATED").setFields(Arrays.asList(
            new TableFieldSchema().setName("place").setType("STRING"),
            new TableFieldSchema().setName("time").setType("TIMESTAMP")
        ))
    ));
    return personTable;
  }

  @Test
  public void testEntityBQTransform_toTableRow() {
    Entity e;
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
    System.out.println("Build transform");
    EntityBQTransform ebt = EntityBQTransform.newBuilder()
        .setStrictCast(true)
        .setRowSchema(exampleTable().getFields())
        .build();

    TableRow r = ebt.toTableRow(e);
    Assert.assertEquals("Colin", r.get("firstName"));
    Assert.assertEquals(25L, r.get("age"));
    Assert.assertEquals("key(SomeKind, 'myKey')", r.get("__key__"));

  }
}
