---
layout: doc_page
---

# Tutorial: Loading a file

## Getting started

This tutorial demonstrates how to perform a batch file load, using Druid's native batch ingestion.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine. You 
don't need to have loaded any data yet.


## Preparing the data and the ingestion task spec

A data load is initiated by submitting an *ingestion task* spec to the Druid overlord. For this tutorial, we'll be loading the sample Wikipedia page edits data.

The Druid package includes the following sample native batch ingestion task spec at `quickstart/wikipedia-index.json`, shown here for convenience,
which has been configured to read the `quickstart/wikipedia-2016-06-27-sampled.json.gz` input file:

```
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "channel",
              "cityName",
              "comment",
              "countryIsoCode",
              "countryName",
              "isAnonymous",
              "isMinor",
              "isNew",
              "isRobot",
              "isUnpatrolled",
              "metroCode",
              "namespace",
              "page",
              "regionIsoCode",
              "regionName",
              "user",
              { "name" : "commentLength", "type" : "long" },
              { "name" : "deltaBucket", "type" : "long" },
              "flags",
              "diffUrl",
              { "name": "added", "type": "long" },
              { "name": "deleted", "type": "long" },
              { "name": "delta", "type": "long" }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "iso"
          }
        }
      },
      "metricsSpec" : [],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2016-06-27/2016-06-28"],
        "rollup" : false
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "quickstart/",
        "filter" : "wikipedia-2016-06-27-sampled.json"
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

This spec will create a datasource named "wikipedia", 

## Load batch data

We've included a sample of Wikipedia edits from September 12, 2015 to get you started.


To load this data into Druid, you can submit an *ingestion task* pointing to the file. We've included
a task that loads the `wikiticker-2015-09-12-sampled.json` file included in the archive. To submit
this task, POST it to Druid in a new terminal window from the druid-#{DRUIDVERSION} directory:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/wikiticker-index.json localhost:8090/druid/indexer/v1/task
```

Which will print the ID of the task if the submission was successful:

```bash
{"task":"index_hadoop_wikipedia_2013-10-09T21:30:32.802Z"}
```

To view the status of your ingestion task, go to your overlord console:
[http://localhost:8090/console.html](http://localhost:8090/console.html). You can refresh the console periodically, and after
the task is successful, you should see a "SUCCESS" status for the task.

After your ingestion task finishes, the data will be loaded by historical nodes and available for
querying within a minute or two. You can monitor the progress of loading your data in the
coordinator console, by checking whether there is a datasource "wikiticker" with a blue circle
indicating "fully available": [http://localhost:8081/#/](http://localhost:8081/#/).

## Running the task

To actually run this task, first make sure that the indexing task can read *pageviews.json*:

- If you're running locally (no configuration for connecting to Hadoop; this is the default) then 
place it in the root of the Druid distribution.
- If you configured Druid to connect to a Hadoop cluster, upload 
the pageviews.json file to HDFS. You may need to adjust the `paths` in the ingestion spec.

To kick off the indexing process, POST your indexing task to the Druid Overlord. In a standard Druid 
install, the URL is `http://OVERLORD_IP:8090/druid/indexer/v1/task`.

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @my-index-task.json OVERLORD_IP:8090/druid/indexer/v1/task
```

If you're running everything on a single machine, you can use localhost:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @my-index-task.json localhost:8090/druid/indexer/v1/task
```

If anything goes wrong with this task (e.g. it finishes with status FAILED), you can troubleshoot 
by visiting the "Task log" on the [overlord console](http://localhost:8090/console.html).

## Querying your data

Your data should become fully available within a minute or two. You can monitor this process on 
your Coordinator console at [http://localhost:8081/#/](http://localhost:8081/#/).

Once your data is fully available, you can query it using any of the 
[supported query methods](../querying/querying.html).

## Further reading

For more information on loading batch data, please see [the batch ingestion documentation](../ingestion/native-batch.html).
