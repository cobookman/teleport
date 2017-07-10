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
import com.google.cloud.dataflow.teleport.helpers.JSTransform;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.IOException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * GCS to Datastore Records
 */
public class GcsJsonToDatastore {

  /**
   * Runs the GcsJsonToDatastore dataflow pipeline
   */
  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("IngestJson", TextIO.read()
            .from(options.getJsonPathPrefix()))
        .apply("GcsToEntity", ParDo.of(JsonToEntity.newBuilder()
            .setJsTransformPath(options.getJsTransformPath())
            .setJsTransformFunctionName(options.getJsTransformFunctionName())
            .build()))
        .apply(DatastoreIO.v1().write()
            .withProjectId(options.getDatastoreProjectId()));

    pipeline.run();
  }

  interface Options extends GcpOptions {
    @Validation.Required
    @Description("GCS Data Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getJsonPathPrefix();
    void setJsonPathPrefix(ValueProvider<String> jsonPathPrefix);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);

    @Description("Javascript Transform Function Name")
    ValueProvider<String> getJsTransformFunctionName();
    void setJsTransformFunctionName(ValueProvider<String> jsTransformFunctionName);

    @Description("Project to save Datastore Entities in")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);
  }

  /**
   * Converts a Protobuf Encoded Json String to a Datastore Entity
   */
  @AutoValue
  public abstract static class JsonToEntity extends DoFn<String, Entity> {
    private JsonFormat.Parser mJsonParser;
    private JSTransform mJSTransform;

    abstract ValueProvider<String> jsTransformPath();
    abstract ValueProvider<String> jsTransformFunctionName();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract JsonToEntity.Builder setJsTransformPath(ValueProvider<String> jsTransformPath);
      public abstract JsonToEntity.Builder setJsTransformFunctionName(ValueProvider<String> jsTransformFunctionName);
      public abstract JsonToEntity build();
    }

    public static Builder newBuilder() {
      return new com.google.cloud.dataflow.teleport.AutoValue_GcsJsonToDatastore_JsonToEntity.Builder();
    }

    private JsonFormat.Parser getJsonParser() {
      if (mJsonParser == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonParser = JsonFormat.parser()
            .usingTypeRegistry(typeRegistry);
      }
      return mJsonParser;
    }

    private JSTransform getJSTransform() {
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
      String entityJson = c.element();

      if (getJSTransform().hasTransform()) {
        entityJson = getJSTransform().invoke(entityJson);
      }

      Entity.Builder builder = Entity.newBuilder();
      getJsonParser().merge(entityJson, builder);
      Entity entity = builder.build();

      // Remove old project id reference from key
      Key k = entity.getKey();
      builder.setKey(Key.newBuilder()
          .addAllPath(k.getPathList())
          .setPartitionId(PartitionId.newBuilder()
              .setNamespaceId(k.getPartitionId().getNamespaceId()))
          .build());
      c.output(builder.build());
    }
  }
}
