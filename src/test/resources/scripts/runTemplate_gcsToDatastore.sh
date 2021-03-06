#!/usr/bin/env bash
#  Copyright 2017 Google Inc.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../../../../

DATAFLOW_PROJECT="teleport-test-170818"
DATASTORE_PROJECT="teleport-test-170818"
DATA_PATH="gs://teleport-test/backups/*.json"
TEMPLATE="gs://teleport-test/templates/gcsToDatastore"
GCS_TRANSFORM="gs://teleport-test/transforms/gcsToDatastoreTransform.js"
GCS_TRANSFORM_FUNCTION_NAME="transform"
JOB_NAME=""

if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi


gcloud beta dataflow jobs run $JOB_NAME \
  --gcs-location="$TEMPLATE" \
  --project=$DATAFLOW_PROJECT \
  --parameters jsonPathPrefix=$DATA_PATH,datastoreProjectId=$DATASTORE_PROJECT,jsTransformPath=$GCS_TRANSFORM,jsTransformFunctionName=$GCS_TRANSFORM_FUNCTION_NAME
