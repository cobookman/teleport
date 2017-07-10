package com.google.cloud.dataflow.teleport;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Migrates Newline seperated strings stored in various GCS files to Bq
 */
public class GcsToBq {

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("IngestData", TextIO.read())
        .apply("DataToTableRow", ParDo.of(new DataToTableRow(options.getJsTransformPath())));
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

    @Description("Project to save Datastore Entities in")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);
  }

  /**
   * Converts a line of Data to a BQ TableRow.
   */
  static class DataToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
    }
  }
}
