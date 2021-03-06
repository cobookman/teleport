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
package com.google.cloud.dataflow.teleport.Helpers;

import com.eclipsesource.v8.V8ScriptExecutionException;
import com.google.common.base.Strings;
import javax.script.Invocable;
import javax.script.ScriptException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the JSTransform Class
 */
public class JSTransformTest {
  public static final String gcsTransformFns = "gs://teleport-test/test/transforms/";
  public static final String goodGcsTransform = "gs://teleport-test/test/transforms/good";
  public static final String badGcsTransform = "gs://teleport-test/test/transforms/errors";

  @Test
  public void testJSTransform_getScripts() {
    // Listing just javascript files folder gets all scripts
    JSTransform goodTransforms = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .build();

    Assert.assertEquals(2, goodTransforms.getScripts().size());


    // Listing a single js script gets it
    JSTransform singleTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform + "/transform.js")
        .build();

    Assert.assertEquals(1, singleTransform.getScripts().size());


    // Listing a directory with more than just js gets just js
    JSTransform folderTransforms = JSTransform.newBuilder()
        .setGcsJSPath(gcsTransformFns)
        .build();

    Assert.assertEquals(4, folderTransforms.getScripts().size());

    // Transform Fns are non null strings
    for (String s : folderTransforms.getScripts()) {
      Assert.assertFalse(Strings.isNullOrEmpty(s));
    }
  }


  @Test
  public void testJSTransform_invoke() throws ScriptException, NoSuchMethodException {
    // Test JS Transform involving multiple files
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .setFunctionName("transform")
        .build();

    String output = (String) jsTransform.invoke("{\"key\": \"value\"}");
    String expected = "{\"Some Property\":\"Some Key\",\"entity jsonified\":\"{\\\"key\\\": \\\"value\\\"}\"}";
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testJSTransform_getInvocable() {
    JSTransform jsTransform;
    // Test a good invocable
    jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .build();

    jsTransform.getInvocable();

    // Test an invocable that should throw an exception
    V8ScriptExecutionException ve = null;
    try {
      jsTransform = JSTransform.newBuilder()
          .setGcsJSPath(badGcsTransform)
          .build();

      jsTransform.getInvocable();
    } catch(V8ScriptExecutionException e) {
      ve = e;
    }

    Assert.assertNotNull(ve);
  }

  @Test
  public void testJSTransform_hasTransform() throws ScriptException {
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath("")
        .build();
    Assert.assertFalse(jsTransform.hasTransform());
  }
}
