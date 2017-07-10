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

DATAFLOW_PROJECT=""
DATASTORE_PROJECT=""
DATA_PATH=""
TEMPLATE=""
GCS_TRANSFORM=""
GCS_TRANSFORM_FUNCTION_NAME=""
JOB_NAME=""


if [[ -z $DATAFLOW_PROJECT ]]; then
  echo -n "Project Id to run dataflow job in (my-project-id): "
  read DATAFLOW_PROJECT
fi

if [[ -z $DATASTORE_PROJECT ]]; then
  echo -n "Project Id to pull Datastore Entities From (my-project-id): "
  read DATASTORE_PROJECT
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where is the Dataflow Template Located (gs://mybucket/templates/gcsToDatastore): "
  read TEMPLATE
fi

if [[ -z $DATA_PATH ]]; then
  echo -n "GCS path of data to be read in (gs://mybucket/data/*.json): "
  read DATA_PATH
fi

if [[ -z $GCS_TRANSFORM ]]; then
  echo -n "What is the GCS path of the javascript transform (gs://mybucket/transforms/): "
  read GCS_TRANSFORM
fi

if [[ -z $GCS_TRANSFORM_FUNCTION_NAME ]]; then
  echo -n "What is the Javascript Transform Function Name: "
  read GCS_TRANSFORM_FUNCTION_NAME
fi

if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi


gcloud beta dataflow jobs run $JOB_NAME \
  --gcs-location="$TEMPLATE" \
  --project=$DATAFLOW_PROJECT \
  --parameters jsonPathPrefix=$DATA_PATH,datastoreProjectId=$DATASTORE_PROJECT,jsTransformPath=$GCS_TRANSFORM,jsTransformFunctionName=$GCS_TRANSFORM_FUNCTION_NAME
