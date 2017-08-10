package com.google.cloud.dataflow.teleport;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.teleport.GcsJsonToBq.JsonToTableRow;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 7/7/17.
 */
public class GcsJsonToBqTest {
  public static final String mJson = "{\"strKey\":\"string value\", \"intKey\": 1019}";
  public static final String mTableSchemaJson = "{\"fields\":[{\"name\":\"strKey\",\"type\":\"STRING\"},{\"name\":\"intKey\",\"type\":\"INTEGER\"}]}";
  /**
  @Test
  public void testGcsJsonToBq_testDoFn_noTransform() throws Exception {
    DoFnTester<String, TableRow> fnTester = DoFnTester
        .of(JsonToTableRow.newBuilder()
            .setTableSchemaJson(StaticValueProvider.of(mTableSchemaJson))
            .setJsTransformPath(StaticValueProvider.of(null))
            .setJsTransformFunctionName(StaticValueProvider.of(null))
            .build());

    List<TableRow> rows = fnTester.processBundle(mJson);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("string value", rows.get(0).get("strKey"));
    Assert.assertEquals(1019L, rows.get(0).get("intKey"));
  }  **/

}
