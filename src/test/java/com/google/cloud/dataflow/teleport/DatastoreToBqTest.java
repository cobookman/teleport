package com.google.cloud.dataflow.teleport;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.teleport.DatastoreToBq.EntityToTableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 7/6/17.
 */
public class DatastoreToBqTest {
  public static final String mEntityJson = "{\"key\":{\"partitionId\":{\"projectId\":\"strong-moose\"},\"path\":[{\"kind\":\"Drawing\",\"name\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"}]},\"properties\":{\"points\":{\"arrayValue\":{\"values\":[{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"219\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"242\"},\"x\":{\"integerValue\":\"351\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"255\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"267\"},\"x\":{\"integerValue\":\"347\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"281\"},\"x\":{\"integerValue\":\"345\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"289\"},\"x\":{\"integerValue\":\"344\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"293\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"296\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"298\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"345\"}}}}]}},\"drawingId\":{\"stringValue\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"},\"canvasId\":{\"stringValue\":\"79a1d9d9-e255-427a-9b09-f45157e97790\"}}}";
  public static final String mTableSchemaJson = "{\"fields\":[{\"name\":\"__key__\",\"type\":\"STRING\"},{\"name\":\"canvasId\",\"type\":\"STRING\"},{\"name\":\"drawingId\",\"type\":\"STRING\"},{\"name\":\"points\",\"type\":\"RECORD\",\"mode\":\"REPEATED\",\"fields\":[{\"name\":\"x\",\"type\":\"INTEGER\"},{\"name\":\"y\",\"type\":\"FLOAT\"}]}]}";

  @Test
  public void testDatastoreToBq_EntityToTableRow_notransform() throws Exception, IOException {
    DoFnTester<Entity, TableRow> fnTester = DoFnTester.of(EntityToTableRow.newBuilder()
        .setStrictCast(StaticValueProvider.of(true))
        .setTableSchemaJson(StaticValueProvider.of(mTableSchemaJson))
        .setJsTransformFunctionName(StaticValueProvider.of(null))
        .setJsTransformPath(StaticValueProvider.of(null))
    .build());

    Builder entityBuilder = Entity.newBuilder();
    JsonFormat.parser().usingTypeRegistry(
        TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build())
        .merge(mEntityJson, entityBuilder);

    Entity entity = entityBuilder.build();
    List<TableRow> tableRows = fnTester.processBundle(entity);
    TableRow tr = tableRows.get(0);

    Assert.assertEquals(1, tableRows.size());
    Assert.assertEquals("key(Drawing, '31ce830e-91d0-405e-855a-abe416cadc1f')", tr.get("__key__"));
    Assert.assertEquals("79a1d9d9-e255-427a-9b09-f45157e97790", tr.get("canvasId"));
  }

}
