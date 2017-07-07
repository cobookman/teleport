package com.google.cloud.dataflow.teleport.Helpers;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Created by bookman on 7/6/17.
 */
public class ValueProviderHelpers {

  public static class GcsLoad implements SerializableFunction<String, String> {

    @Override
    public String apply(String gcspath) {
      Storage storage = StorageOptions.getDefaultInstance().getService();

      String bucketName = gcspath.replace("gs://", "").split("/")[0];
      String blobName = gcspath.replace("gs://" + bucketName + "/", "");

      Blob blob = storage.get(bucketName, blobName);
      if (!blob.exists()) {
        throw new IllegalArgumentException("File does not exists in gcs(" + gcspath + ")");
      }

      return new String(blob.getContent());
    }
  }

}
