#!/usr/bin/env bash

DATAFLOW_PROJECT="teleport-test-170818"
DATASTORE_PROJECT="teleport-test-170818"
DATA_PATH="gs://teleport-test/backups/*.json"
TEMPLATE="gs://teleport-test/templates/gcsToDatastore"
GCS_TRANSFORM="gs://teleport-test/transforms/gcsToDatastoreTransform.js"
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

if [[ -z $DATA_PATH ]]; then
  echo -n "GCS path of data to be read in: "
  read DATA_PATH
fi

if [[ -z $GCS_TRANSFORM ]]; then
  echo -n "What is the GCS path of the javascript transform: "
  read GCS_TRANSFORM
fi


if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi


gcloud beta dataflow jobs run $JOB_NAME \
  --gcs-location="$TEMPLATE" \
  --project=$DATAFLOW_PROJECT \
  --parameters jsonPathPrefix=$DATA_PATH,datastoreProject=$DATASTORE_PROJECT,jsTransformPath=$GCS_TRANSFORM
