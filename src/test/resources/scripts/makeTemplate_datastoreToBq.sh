#!/bin/bash
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

PROJECT="teleport-test-170818"
TEMP="gs://teleport-test/temp/"
TEMPLATE="gs://teleport-test/templates/datastoreToBq"

if [[ -z $PROJECT ]]; then
  echo -n "Project Id hosting the templates (my-project-id): "
  read PROJECT
fi

if [[ -z $TEMP ]]; then
  echo -n "What is the temp location for the dataflow jobs (gs://...): "
  read TEMP
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where to store this template (gs://...): "
  read TEMPLATE
fi

./gradlew clean build shadowJar -x test

java -jar build/libs/shadow-1.0-Alpha.jar \
  datastore_to_bq \
  --project=$PROJECT \
  --runner=DataflowRunner \
  --tempLocation=$TEMP \
  --templateLocation=$TEMPLATE

if [ $? -eq 0 ]; then
  echo "Success! Built Template"
else
  echo "Failed to build Template :'("
fi
