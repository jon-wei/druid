---
layout: doc_page
---

## Writing an ingestion spec

When writing an ingestion spec, the most important questions are:

  * What should the dataset be called? This is the "dataSource" field of the "dataSchema".
  * Where is the dataset located? This configuration belongs in the "ioConfig". The specific configuration
is different for different ingestion methods (local files, Hadoop, Kafka, etc). For the local file method
we're using, the file locations go in the "firehose".
  * Which field should be treated as a timestamp? This belongs in the "column" of the "timestampSpec".
Druid always requires a timestamp column.
  * Do you want to roll up your data as an OLAP cube or not? Druid supports an [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) data model, where you
organize your columns into [dimensions](https://en.wikipedia.org/wiki/Dimension_%28data_warehouse%29) (attributes you can
filter and split on) and [metrics](https://en.wikipedia.org/wiki/Measure_%28data_warehouse%29) (aggregated values; also
called "measures"). OLAP data models are designed to allow fast slice-and-dice analysis of data. To enable rollup, set `"rollup": true` in the "granularitySpec".
  * If you are using an OLAP data model, your dimensions belong in the "dimensions" field of the "dimensionsSpec" and
your metrics belong in the "metricsSpec".
  * If you are not using an OLAP data model, your columns should all go in the "dimensions" field of the
"dimensionsSpec", and the "metricsSpec" should be empty.
  * For batch ingestion only: What time ranges (intervals) are being loaded? This belongs in the "intervals"
of the "granularitySpec".

If your data does not have a natural sense of time, you can tag each row with the current time.
You can also tag all rows with a fixed timestamp, like "2000-01-01T00:00:00.000Z".

Let's use a small pageviews dataset as an example. Druid supports TSV, CSV, and JSON out of the box.
Note that nested JSON objects are not supported, so if you do use JSON, you should provide a file
containing flattened objects.

```json
{"time": "2015-09-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
{"time": "2015-09-01T01:00:00Z", "url": "/", "user": "bob", "latencyMs": 11}
{"time": "2015-09-01T01:30:00Z", "url": "/foo/bar", "user": "bob", "latencyMs": 45}
```

If you save this to a file called "pageviews.json", then for this dataset:

  * Let's call the dataset "pageviews".
  * The data is located in "pageviews.json" in the root of the Imply distribution.
  * The timestamp is the "time" field.
  * Let's use an OLAP data model, so set "rollup" to true.
  * Good choices for dimensions are the string fields "url" and "user".
  * Good choices for measures are a count of pageviews, and the sum of "latencyMs". Collecting that
sum when we load the data will allow us to compute an average at query time as well.
  * The data covers the time range 2015-09-01 (inclusive) through 2015-09-02 (exclusive).

You can copy the existing `quickstart/wikipedia-index.json` indexing task to a new file:

```bash
cp quickstart/wikipedia-index.json my-index-task.json
```

And modify it by altering the sections above. After altering, it should look like:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "pageviews",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "url",
              "user"
            ]
          },
          "timestampSpec" : {
            "format" : "auto",
            "column" : "time"
          }
        }
      },
      "metricsSpec" : [
        { "name": "views", "type": "count" },
        { "name": "latencyMs", "type": "doubleSum", "fieldName": "latencyMs" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2015-09-01/2015-09-02"],
        "rollup" : true
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : ".",
        "filter" : "pageviews.json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index",
      "targetPartitionSize" : 5000000,
      "maxRowsInMemory" : 25000,
      "forceExtendableShardSpecs" : true
    }
  }
}
```


