#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

java -jar avro-tools-1.8.2.jar compile schema avro-schemas/ gen/
