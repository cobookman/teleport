package com.google.cloud.dataflow.teleport;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.teleport.Helpers.EntityBQTransform;
import com.google.cloud.dataflow.teleport.Helpers.JSTransform;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Exports Datastore Entities to BigQueryHelper
 */
public class DatastoreToBq {

  /**
   * Runs the DatastoreToBigQuery dataflow pipeline
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
        .apply("EntityToTableRow", ParDo.of(EntityToTableRow.newBuilder()
            .setJsTransformPath(options.getJsTransformPath())
            .setStrictCast(options.getStrictCast())
            .setTableSchemaJson(options.getBQJsonSchema())
            .build()))
        .apply("TableRowToBigQuery", BigQueryIO.writeTableRows()
            .to(options.getBQTableSpec())
            .withJsonSchema(options.getBQJsonSchema())
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    pipeline.run();
  }

  interface Options extends PipelineOptions {

    @Validation.Required
    @Description("GQL Query to specify which datastore Entities")
    ValueProvider<String> getGqlQuery();
    void setGqlQuery(ValueProvider<String> gqlQuery);

    @Description("Project to grab Datastore Entities from")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Validation.Required
    @Description("Namespace of requested Entities, use `\"\"` for default")
    ValueProvider<String> getNamespace();
    void setNamespace(ValueProvider<String> namespace);

    @Validation.Required
    @Description("BigQuery Destination Table Spec ([project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
    ValueProvider<String> getBQTableSpec();
    void setBQTableSpec(ValueProvider<String> bqTableSpec);

    /**
     * A TableSchema Object serialized as Json
     *
     * Example:
     * <pre>
     * {
     *   "fields":[
     *     {"name":"someName", "type":"STRING"},
     *     {"name":"someOtherName", "type":"BOOLEAN"},
     *     {
     *       "name": "someOtherOtherName", "type":"RECORD",
     *       "fields":[
     *         {"name":"someSubField", "type":"STRING"},
     *         {"name":"someOtherSubField", "type":"INTEGER"},
     *         {"name": "someFloat", "type": "FLOAT"},
     *         {"name": "someTimestamp", "type": "TIMESTAMP"},
     *         {"name": "someDate", "type": "DATE"},
     *         {"name": "someTime", "type": "TIME"},
     *         {"name": "someDateTime", "type": "DATETIME"},
     *         {"name": "someBytes", "type": "BYTES"}
     *       ]
     *     }
     *   ]
     * }
     * </pre>
     * @return a ValueProvider containing the BQ Table Json Schema
     */
    @Validation.Required
    @Description("BigQuery Table Schema in Json")
    ValueProvider<String> getBQJsonSchema();
    void setBQJsonSchema(ValueProvider<String> bqJsonSchema);

    @Description("Should do a strict Datastore Entity to BQ Table Row cast")
    @Default.Boolean(false)
    ValueProvider<Boolean> getStrictCast();
    void setStrictCast(ValueProvider<Boolean> strictCast);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);
  }

  /**
   * Converts a Datstore Entity to BigQuery Table Row
   */
  @AutoValue
  public abstract static class EntityToTableRow extends DoFn<Entity, TableRow> {
    private JSTransform mJSTransform;
    private TableSchema mTableSchema;
    private Gson mGson = new Gson();

    abstract ValueProvider<String> getJsTransformPath();
    abstract ValueProvider<String> getTableSchemaJson();
    abstract ValueProvider<Boolean> getStrictCast();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract EntityToTableRow.Builder setJsTransformPath(ValueProvider<String> jsTransformPath);
      public abstract EntityToTableRow.Builder setTableSchemaJson(ValueProvider<String> tableSchemaJson);
      public abstract EntityToTableRow.Builder setStrictCast(ValueProvider<Boolean> strictCast);
      public abstract EntityToTableRow build();
    }

    public static Builder newBuilder() {
      return com.google.cloud.dataflow.teleport.AutoValue_DatastoreToBq_EntityToTableRow.newBuilder();
    }

    private TableSchema getTableSchema() {
      if (mTableSchema == null) {
        Gson gson = new Gson();
        mTableSchema = gson.fromJson(getTableSchemaJson().get(), TableSchema.class);
      }
      return mTableSchema;
    }

    private JSTransform getJSTransform() throws ScriptException {
      if (mJSTransform == null) {
        String jsTransformPath = "";
        if (getJsTransformPath().isAccessible()) {
          jsTransformPath = getJsTransformPath().get();
        }

        mJSTransform = JSTransform.newBuilder()
            .setGcsJSPath(jsTransformPath)
            .build();
      }
      return mJSTransform;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Entity entity = c.element();
      TableSchema ts = getTableSchema();
      EntityBQTransform ebt = EntityBQTransform.newBuilder()
          .setRowSchema(getTableSchema().getFields())
          .setStrictCast(getStrictCast().get())
          .build();

      TableRow row = ebt.toTableRow(entity);

      if (getJSTransform().hasTransform()) {
        String rowJson = getJSTransform().invoke(mGson.toJson(row));
        row = mGson.fromJson(rowJson, TableRow.class);
      }

      c.output(row);
    }
  }
}
