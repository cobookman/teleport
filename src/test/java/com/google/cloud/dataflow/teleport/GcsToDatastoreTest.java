/*
  Copyright 2017 Google Inc.
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.GcsToDatastore.JsonToEntity;
import com.google.datastore.v1.Entity;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests GcsToDatastore class.
 */
public class GcsToDatastoreTest {
  public static final String mEntityJson = "{\"key\":{\"partitionId\":{},\"path\":[{\"kind\":\"Drawing\",\"name\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"}]},\"properties\":{\"points\":{\"arrayValue\":{\"values\":[{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"219\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"242\"},\"x\":{\"integerValue\":\"351\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"255\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"267\"},\"x\":{\"integerValue\":\"347\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"281\"},\"x\":{\"integerValue\":\"345\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"289\"},\"x\":{\"integerValue\":\"344\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"293\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"296\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"298\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"345\"}}}}]}},\"drawingId\":{\"stringValue\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"},\"canvasId\":{\"stringValue\":\"79a1d9d9-e255-427a-9b09-f45157e97790\"}}}";

  @Test
  public void testGcsToDatastore_EntityToJson_noTransform() throws Exception {
    DoFnTester<String, Entity> fnTester = DoFnTester.of(JsonToEntity.newBuilder()
        .setJsTransformPath(StaticValueProvider.of(null))
        .setJsTransformFunctionName(StaticValueProvider.of(null))
        .build());
    List<Entity> output = fnTester.processBundle(mEntityJson);
    Entity outputEntity = output.get(0);

    Printer printer = JsonFormat.printer()
        .omittingInsignificantWhitespace()
        .usingTypeRegistry(
            TypeRegistry.newBuilder()
                .add(Entity.getDescriptor())
                .build());
    Assert.assertEquals(mEntityJson, printer.print(outputEntity));
  }
}
