Dataflow Teleport
==========================

[![Build
Status](https://travis-ci.org/cobookman/teleport.svg?branch=master)](https://travis-ci.org/cobookman/teleport)

In this repo you'll find Dataflow templates which move data from one place to
another.


# Quickstart

1) Clone this repo

2) Upload templates to GCS
```bash
$ ./scripts/makeTemplate_datastoreToGcs.sh

...follow prompts...

$ ./scripts/makeTemplate_gcsToDatastore.sh

...follow prompts...
```

3) Run Jobs

3.1) Datastore-\>GCS: `$ ./scripts/runTemplate_datastoreToGcs.sh`

3.2) GCS-\>Datastore: `$ ./scripts/runTemplate_gcsToDatastore.sh`


# Transforms

All jobs offer the ability to transform the data before loading it into its new
destination. This is done through javascript functions. The Dataflow job 
call a javascript transform before loading the data into the new data source.

Your javascript code must contain a function called transform which takes in a single
String parameter. This function must also return a String.


For example say you
wanted to dump your datastore data as CSV instead of json. You'd upload the
following transform file to GCS

```javascript
/**
 * This function transforms an entity to csv (deliminated by '|'). It expects
 * an entity that looks like the following schema:
 * \`\`\`{"key":{"partitionId":{"projectId":"teleport-test-170818"},"path":[{"kind":"SomeKind","name":"mykey"}]},
 * "properties":{"someBool":{"booleanValue":true},"someText":{"meaning":15,"stringValue":"some long text value.",
 * "excludeFromIndexes":true},"someNull":{"nullValue":null}, "someDateTime":{"timestampValue":"2017-06-16T20:46:09.073Z"},
 * "someKey":{"keyValue":{"partitionId":{"projectId":"teleport-test-170818"},
 * "path":[{"kind":"SomeKind","name":"someId"}]}},"someInteger":{"integerValue":"23"},
 * "someArray":{"arrayValue":{"values":[{"stringValue":"arrValue1"},{"stringValue":"arrValue2"}]}},
 *  "someprop":{"stringValue":"some value\t"},"someGeoPoint":{"geoPointValue":{"latitude":90,"longitude":-90}},
 *  "someFloat":{"doubleValue":0.234}},"transformProperty":1497646921672}\`\`\`
 */
function transform(entityJson) {
  var entity = JSON.parse(entityJson);

  return [
    entity.key.partitionId.projectId,
    entity.key.path.map(function(path) {
      return path.name || path.id;
    }).join("/"),
    entity.key.path.map(function(path) {
      return path.kind;
    }).join("/"),
    entity.properties.someBool.booleanValue,
    entity.properties.someArray.arrayValue.values.map(function(value) {
      return value.stringValue;
    }).join(","),
    entity.properties.someText.stringValue,
    entity.properties.someDateTime.timestampValue,
    entity.properties.someFloat.doubleValue,
    entity.properties.someInteger.integerValue
  ].join("|");
}
```


## Multiple Javascript Files

Your javascript code for the transform function does not need to live in a
single file. For example say you had the javascript files of:

  * `gs://mybucket/transforms/datastoreToGcs/file1.js`
  * `gs://mybucket/transforms/datastoreToGcs/file2.js`

Using the paramter of `--jsTransformPath=gs://mybucket/transforms/datastoreToGcs/`
would load both `file1.js` and `file2.js`.


## Javascript Support

The Java Javascript engine that is used supports all of ECMAScript 5.1, and does
not support arrow funcitons or promises. You will also be unable to make any
XMLHttpRequest calls in your javascript code. If you're curious for further
limitations lookup information on the 'Nashorn' javascript engine.


# DatastoreToGcs

This template simply reads datastore entities and writes them to GCS. The
datstore entities are encoded as [protobuf
json](https://developers.google.com/protocol-buffers/docs/proto3#json).

## Arguments

These are the arguments that are optional / necessary for running the template:

* --savePath
  * Where to save your entities, this should be a unique path prefix.
  * Example: `gs://mybackups/datastore/20170101-MyKind`

* --gqlQuery
  * a [GQL](https://cloud.google.com/datastore/docs/reference/gql_reference)
    statement used to grab entity data.
  * Example: `"SELECT * FROM MyKind"`

* --datastoreProject
  * Project that hosts your datastore entities. This is likely the same as the
    project running your dataflow job.

* --jsTransformPath
  * This paramter is optional. Leaving it out, or setting it to "" will cause no
    transform to run.
  * Path Prefix to your Javascript Transform Function.
  * Can be a path direct to a file to load a single javascript file, or a path
    to a folder to recursively load all the javascript files.


# GCSToDatastore

This template reads in newline seperated json strings in GCS, runs a transform,
then parsing & saving the JSON as Datstore Entities.

After your transform you need to have your Json follow the [Entity Json
Spec](https://cloud.google.com/datastore/docs/reference/rest/v1/Entity).

Here's an example json encoding of a Datastore Entity:

```json
{
  "key": {
    "partitionId": {
      "projectId": "teleport-test-170818"
    },
    "path": [
      {
        "kind": "SomeKind",
        "name": "mykey"
      }
    ]
  },
  "properties": {
    "someBool": {
      "booleanValue": true
    },
    "someText": {
      "meaning": 15,
      "stringValue": "some long text value.",
      "excludeFromIndexes": true
    },
    "someNull": {
      "nullValue": null
    },
    "someDateTime": {
      "timestampValue": "2017-06-16T20:46:09.073Z"
    },
    "someKey": {
      "keyValue": {
        "partitionId": {
          "projectId": "teleport-test-170818"
        },
        "path": [
          {
            "kind": "SomeKind",
            "name": "someId"
          }
        ]
      }
    },
    "someInteger": {
      "integerValue": "23"
    },
    "someArray": {
      "arrayValue": {
        "values": [
          {
            "stringValue": "arrValue1"
          },
          {
            "stringValue": "arrValue2"
          }
        ]
      }
    },
    "someprop": {
      "stringValue": "some value\t"
    },
    "someGeoPoint": {
      "geoPointValue": {
        "latitude": 90,
        "longitude": -90
      }
    },
    "someFloat": {
      "doubleValue": 0.234
    }
  },
  "transformProperty": 1497646921672
}
```

## Arguments

* --jsonPathPrefix
  * Where the newline seperated json is located. 
  * Example: `gs://mybackups/datastore/20170101-MyKind`

* --datastoreProject
  * Project that hosts your datastore entities. This is likely the same as the
    project running your dataflow job.

* --jsTransformPath
  * This paramter is optional. Leaving it out, or setting it to "" will cause no
    transform to run.
  * Path Prefix to your Javascript Transform Function.
  * Can be a path direct to a file to load a single javascript file, or a path
    to a folder to recursively load all the javascript files.



