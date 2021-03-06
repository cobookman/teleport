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

import com.eclipsesource.v8.V8;
import com.eclipsesource.v8.V8Array;
import com.eclipsesource.v8.V8ScriptExecutionException;
import com.google.api.gax.paging.Page;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.script.ScriptException;

/**
 * Handles all Javascript Transform related aspects
 */
@AutoValue
public abstract class JSTransform {
  private V8 mRuntime;

  @Nullable abstract String gcsJSPath();
  @Nullable abstract String functionName();
  abstract Optional<String> project();

  public static Builder newBuilder() {
    return new com.google.cloud.dataflow.teleport.Helpers.AutoValue_JSTransform.Builder()
        .setFunctionName("transform")
        .setGcsJSPath("");
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setGcsJSPath(String gcsJSPath);
    public abstract Builder setFunctionName(String functionName);
    public abstract Builder setProject(Optional<String> project);
    public abstract JSTransform build();
  }

  private Storage getStorageService() {
    StorageOptions storageOptions = StorageOptions.getDefaultInstance();
    if (project().isPresent()) {
      storageOptions = StorageOptions.newBuilder()
          .setProjectId(project().get())
          .build();
    }
    return storageOptions.getService();
  }

  /**
   * Gives the raw JS Scripts sepcified
   * @return list of raw JS Script data as strings
   */
  public List<String> getScripts() {
    if (Strings.isNullOrEmpty(gcsJSPath())) {
      return new ArrayList<>();
    }

    String bucketName = gcsJSPath().replace("gs://", "").split("/")[0];
    String prefixPath = gcsJSPath().replace("gs://" + bucketName + "/", "");

    Bucket bucket = getStorageService().get(bucketName);
    if (bucket == null || !bucket.exists()) {
      throw new IllegalArgumentException(
          "Bucket does not exist, or do not have adequate permissions");
    }

    ArrayList<String> filePaths = new ArrayList<>();
    if (prefixPath.endsWith(".js")) {
      filePaths.add(prefixPath);
    } else {
      Page<Blob> blobs = bucket.list(BlobListOption.prefix(prefixPath));
      blobs.iterateAll().forEach((Blob blob) -> {
        if (blob.getName().endsWith(".js")) {
          filePaths.add(blob.getName());
        }
      });
    }

    List<String> scripts = new ArrayList<>();
    for (String filePath : filePaths) {
      Blob b = bucket.get(filePath);
      if (b == null || !b.exists()) {
        throw new IllegalArgumentException(
            "File does not exist, or do not have adequate permissions");
      }
      scripts.add(new String(b.getContent()));
    }

    return scripts;
  }

  public String invoke(Object... params) throws ScriptException, NoSuchMethodException, V8ScriptExecutionException {
    V8Array transformParams = new V8Array(getInvocable());
    for (Object param : params) {
      transformParams.push(param);
    }

    return getInvocable().executeStringFunction(functionName(), transformParams);
  }


  public boolean hasTransform() throws ScriptException {
    return (getInvocable() != null);
  }

  @Nullable
  public V8 getInvocable() {
    if (Strings.isNullOrEmpty(gcsJSPath())) {
      return null;
    }

    if (mRuntime == null) {
      V8 runtime = V8.createV8Runtime();
      for (String script : getScripts()) {
        runtime.executeScript(script);
      }
      mRuntime = runtime;
    }
    return mRuntime;
  }
}
