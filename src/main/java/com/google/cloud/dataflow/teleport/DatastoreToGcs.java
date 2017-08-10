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

import avro.com.google.datastore.v1.ArrayValue;
import avro.com.google.datastore.v1.Key;
import avro.com.google.datastore.v1.LatLng;
import avro.com.google.datastore.v1.PartitionId;
import avro.com.google.datastore.v1.PathElement;
import avro.com.google.datastore.v1.Value;

import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.AvroIO;
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
  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    Schema schema = avro.com.google.datastore.v1.Entity.getClassSchema();
    pipeline
        .apply("IngestEntities",
            DatastoreIO.v1().read()
                .withProjectId(options.getDatastoreProjectId())
                .withLiteralGqlQuery(options.getGqlQuery())
                .withNamespace(options.getNamespace()))
        .apply("EntityToAvroEntity", ParDo.of(new EntityToAvro()))
        .apply("AvroToGCS", AvroIO.writeGenericRecords(schema.toString())
            .to(options.getSavePath())
            .withSuffix(".avro"));

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
  }

  /**
   * Converts a Datstore Entity to AvroEntity
   */
  public static class EntityToAvro extends DoFn<Entity, GenericRecord> {

    public avro.com.google.datastore.v1.Entity entityToAvroEntity(Entity entity) {
      Map<CharSequence, Value> values = new HashMap<>();
      for (Entry<String, com.google.datastore.v1.Value> entry : entity.getPropertiesMap().entrySet()) {
        values.put(entry.getKey(), valueToAvroValue(entry.getValue()));
      }

      return avro.com.google.datastore.v1.Entity.newBuilder()
          .setKey(keyToAvroKey(entity.getKey()))
          .setValues(values)
          .build();
    }

    public Key keyToAvroKey(com.google.datastore.v1.Key k) {
      List<PathElement> path = new ArrayList<>();
      for (com.google.datastore.v1.Key.PathElement pathElm : k.getPathList()) {
        PathElement.Builder builder = PathElement.newBuilder();
        switch (pathElm.getIdTypeCase()) {
          case ID:
            builder.setId(pathElm.getId());
            break;
          case NAME:
            builder.setName(pathElm.getName());
            break;
        }
        builder.setKind(pathElm.getKind());
        path.add(builder.build());
      }

      return Key.newBuilder()
          .setPartitionIdBuilder(PartitionId.newBuilder()
              .setNamespaceId(k.getPartitionId().getNamespaceId())
              .setProjectId(k.getPartitionId().getProjectId()))
          .setPath(path)
          .build();
    }

    public Value valueToAvroValue(com.google.datastore.v1.Value v) {
      Value.Builder builder = null;
      switch (v.getValueTypeCase()) {
        case INTEGER_VALUE:
          builder = Value.newBuilder().setIntegerValue(v.getIntegerValue());
          break;
        case BOOLEAN_VALUE:
          builder = Value.newBuilder().setBooleanValue(v.getBooleanValue());
          break;
        case DOUBLE_VALUE:
          builder = Value.newBuilder().setDoubleValue(v.getDoubleValue());
          break;
        case STRING_VALUE:
          builder = Value.newBuilder().setStringValue(v.getStringValue());
          break;
        case GEO_POINT_VALUE:
          builder = Value.newBuilder().setGeoPointValueBuilder(LatLng.newBuilder()
              .setLatitude(v.getGeoPointValue().getLatitude())
              .setLongitude(v.getGeoPointValue().getLongitude()));
          break;
        case BLOB_VALUE:
          builder = Value.newBuilder().setBlobValue(v.getBlobValue().asReadOnlyByteBuffer());
          break;
        case ARRAY_VALUE:
          List<com.google.datastore.v1.Value> valList = v.getArrayValue().getValuesList();
          List<Value> avroVals = Arrays.asList(new Value[valList.size()]);
          for(int i = 0; i < avroVals.size(); ++i) {
            avroVals.set(i, valueToAvroValue(valList.get(i)));
          }
          builder =  Value.newBuilder().setArrayValueBuilder(
              ArrayValue.newBuilder().setValues(avroVals));
          break;
        case ENTITY_VALUE:
          Entity e = v.getEntityValue();
          builder = Value.newBuilder().setEntityValue(entityToAvroEntity(e));
          break;
        case TIMESTAMP_VALUE:
          builder = Value.newBuilder().setTimestampValue(
              Timestamps.toMicros(v.getTimestampValue()));
          break;
        case KEY_VALUE:
          builder = Value.newBuilder().setKeyValue(keyToAvroKey(v.getKeyValue()));
          break;

        /**
         * Null must be the last value
         */
        case NULL_VALUE:
          builder = Value.newBuilder().setNullValue(null);
          break;
      }

      if (builder == null) {
        throw new IllegalArgumentException("type case not supported");
      }

      return builder.build();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(entityToAvroEntity(c.element()));
    }
  }

}
