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

DATAFLOW_PROJECT="teleport-test-170818"
DATASTORE_PROJECT="teleport-test-170818"
TEMPLATE="gs://teleport-test/templates/datastoreToGcs"
SAVE_PATH="gs://teleport-test/backups/"
GCS_TRANSFORM="gs://teleport-test/transforms/datastoreToGcsTransform.js"
GQL="SELECT * FROM SomeKind"
JOB_NAME=""


if [[ -z $DATAFLOW_PROJECT ]]; then
  echo -n "Project to run dataflow job in: "
  read DATAFLOW_PROJECT
fi

if [[ -z $DATASTORE_PROJECT ]]; then
  echo -n "Project to pull Datastore Entities From: "
  read DATASTORE_PROJECT
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where is the Dataflow Template Located: "
  read TEMPLATE
fi

if [[ -z $SAVE_PATH ]]; then
  echo -n "Where to save datstore entities: "
  read SAVE_PATH
fi

if [[ -z $GCS_TRANSFORM ]]; then
  echo -n "What is the GCS path of the javascript transform: "
  read GCS_TRANSFORM
fi

if [[ -z $GQL ]]; then
  echo -n "GQL Query of datastore entities to fetch: "
  read GQL
fi

if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi


gcloud beta dataflow jobs run $JOB_NAME \
  --gcs-location="$TEMPLATE" \
  --project=$DATAFLOW_PROJECT \
  --parameters savePath="$SAVE_PATH",gqlQuery="$GQL",datastoreProject=$DATASTORE_PROJECT,jsTransformPath=$GCS_TRANSFORM
