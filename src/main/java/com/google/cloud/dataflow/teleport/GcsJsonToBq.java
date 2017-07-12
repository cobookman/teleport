package com.google.cloud.dataflow.teleport;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.teleport.helpers.JSTransform;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Transport;

/**
 * Migrates Newline seperated strings stored in various GCS files to Bq
 */
public class GcsJsonToBq {

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("IngestData", TextIO.read())
        .apply("JsonToTableRow", ParDo.of(JsonToTableRow.newBuilder()
            .setJsTransformFunctionName(options.getJsTransformFunctionName())
            .setJsTransformPath(options.getJsTransformPath())
            .setTableSchemaJson(options.getBqJsonSchema())
            .build()))
        .apply("TableRowToBigQuery", BigQueryIO.writeTableRows()
            .to(options.getBqTableSpec())
            .withJsonSchema(options.getBqJsonSchema())
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }

  interface Options extends GcpOptions {
    @Validation.Required
    @Description("BigQuery Destination Table Spec ([project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
    ValueProvider<String> getBqTableSpec();
    void setBqTableSpec(ValueProvider<String> bqTableSpec);

    /**
     * A TableSchema Object serialized as Json stored in GCS
     *
     * Example:
     * <pre>
     * {
     *   "fields":[
     *     {"name":"someName", "type":"STRING"},
     *     {"name":"someOtherName", "type":"BOOLEAN"},
     *     {
     *       "name": "someOtherOtherName", "type":"RECORD", "mode": "REPEATED",
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
    @Description("GCS Path for BigQuery Table Schema in Json")
    ValueProvider<String> getBqJsonSchema();
    void setBqJsonSchema(ValueProvider<String> bqJsonSchema);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);

    @Description("Javascript Transform Function Name")
    ValueProvider<String> getJsTransformFunctionName();
    void setJsTransformFunctionName(ValueProvider<String> jsTransformFunctionName);
  }

  /**
   * Converts Json to a BQ TableRow
   */
  @AutoValue
  public abstract static class JsonToTableRow extends DoFn<String, TableRow> {
    private JSTransform mJSTransform;

    abstract ValueProvider<String> jsTransformPath();
    abstract ValueProvider<String> jsTransformFunctionName();
    abstract ValueProvider<String> tableSchemaJson();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract JsonToTableRow.Builder setJsTransformPath(
          ValueProvider<String> jsTransformPath);

      public abstract JsonToTableRow.Builder setJsTransformFunctionName(
          ValueProvider<String> jsTransformFunctionName);

      public abstract JsonToTableRow.Builder setTableSchemaJson(
          ValueProvider<String> tableSchemaJson);

      public abstract JsonToTableRow build();
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

    public static JsonToTableRow.Builder newBuilder() {
      return new com.google.cloud.dataflow.teleport.AutoValue_GcsJsonToBq_JsonToTableRow.Builder();
    }


    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String json = c.element();
      if (getJSTransform().hasTransform()) {
        json = (String) getJSTransform().invoke(json, tableSchemaJson().get());
      }
      TableRow row = Transport.getJsonFactory().fromString(json, TableRow.class);
      c.output(row);
    }
  }
}
