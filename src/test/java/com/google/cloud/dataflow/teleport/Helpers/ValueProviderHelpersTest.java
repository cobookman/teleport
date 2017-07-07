package com.google.cloud.dataflow.teleport.Helpers;

import com.google.cloud.dataflow.teleport.Helpers.ValueProviderHelpers.GcsLoad;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 7/6/17.
 */
public class ValueProviderHelpersTest {

  @Test
  public void testGcsLoad() {
    NestedValueProvider<String, String> vp = NestedValueProvider
        .of(StaticValueProvider.of("gs://teleport-test/schema/SomeOtherKind.schema.json"),
            new GcsLoad());
    Assert.assertEquals("{\"fields\":[{\"name\":\"__key__\",\"type\":\"STRING\"},{\"name\":\"canvasId\",\"type\":\"STRING\"},{\"name\":\"drawingId\",\"type\":\"STRING\"},{\"name\":\"points\",\"type\":\"RECORD\",\"mode\":\"REPEATED\",\"fields\":[{\"name\":\"x\",\"type\":\"FLOAT\"},{\"name\":\"y\",\"type\":\"FLOAT\"}]}]}",
        vp.get().replace(" ", "").replace("\n", ""));

  }
}
