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

import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.teleport.Helpers.JSTransform;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import javax.script.ScriptException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.datastore.v1.Entity;
import java.io.IOException;

/**
 * Exports Datastore Entities to GCS as newline deliminted Protobuf v3 Json.
 */
public class DatastoreToGcs {

  /**
   * Runs the DatastoreToGcs dataflow pipeline
   */
  public static void main(String[] args) throws IOException, ScriptException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("IngestEntities",
            DatastoreIO.v1().read()
                .withProjectId(options.getDatastoreProjectId())
                .withLiteralGqlQuery(options.getGqlQuery())
                .withNamespace(options.getNamespace()))
        .apply("EntityToJson", ParDo.of(EntityToJson.newBuilder()
            .setJsTransformPath(options.getJsTransformPath())
            .setJsTransformFunctionName(options.getJsTransformFunctionName())
            .build()))
        .apply("JsonToGcs", TextIO.write().to(options.getSavePath())
            .withSuffix(".json"));

    pipeline.run();
  }

  interface Options extends PipelineOptions {

    @Validation.Required
    @Description("GCS Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getSavePath();
    void setSavePath(ValueProvider<String> savePath);

    @Validation.Required
    @Description("GQL Query to specify which datastore Entities")
    ValueProvider<String> getGqlQuery();
    void setGqlQuery(ValueProvider<String> gqlQuery);

    @Description("Project to grab Datastore Entities from")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("Namespace of requested Entities, use `\"\"` for default")
    @Default.String("")
    ValueProvider<String> getNamespace();
    void setNamespace(ValueProvider<String> namespace);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);

    @Description("Javascript Transform Function Name")
    ValueProvider<String> getJsTransformFunctionName();
    void setJsTransformFunctionName(ValueProvider<String> jsTransformFunctionName);
  }

  /**
   * Converts a Datstore Entity to Protobuf encoded Json
   */
  @AutoValue
  public abstract static class EntityToJson extends DoFn<Entity, String> {
    private JsonFormat.Printer mJsonPrinter;
    private JSTransform mJSTransform;

    abstract ValueProvider<String> jsTransformPath();
    abstract ValueProvider<String> jsTransformFunctionName();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract EntityToJson.Builder setJsTransformPath(ValueProvider<String> jsTransformPath);
      public abstract EntityToJson.Builder setJsTransformFunctionName(ValueProvider<String> jsTransformFunctionName);
      public abstract EntityToJson build();
    }

    public static Builder newBuilder() {
      return new com.google.cloud.dataflow.teleport.AutoValue_DatastoreToGcs_EntityToJson.Builder();
    }

    private JsonFormat.Printer getJsonPrinter() {
      if (mJsonPrinter == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonPrinter = JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .omittingInsignificantWhitespace();
      }
      return mJsonPrinter;
    }

    private JSTransform getJSTransform() throws ScriptException {
      if (mJSTransform == null) {
        JSTransform.Builder jsTransformBuilder = JSTransform.newBuilder();
        if (jsTransformPath().isAccessible()) {
          jsTransformBuilder.setGcsJSPath(jsTransformPath().get());
        }

        if (jsTransformFunctionName().isAccessible()) {
          jsTransformBuilder.setFunctionName(jsTransformFunctionName().get());
        }

        mJSTransform = jsTransformBuilder.build();
      }
      return mJSTransform;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Entity entity = c.element();
      String json = getJsonPrinter().print(entity);

      if (getJSTransform().hasTransform()) {
        json = getJSTransform().invoke(json);
      }

      c.output(json);
    }
  }

}
