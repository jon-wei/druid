---
layout: doc_page
---

# Tutorial: Load streaming data with Tranquility

## Getting started

This tutorial shows you how to load streaming data into Druid using Tranquility Server.

[Tranquility Server](https://github.com/druid-io/tranquility/blob/master/docs/server.md) allows a stream of data to be pushed into Druid using HTTP POSTs.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [single-machine quickstart](quickstart.html) and have it running on your local machine. You
don't need to have loaded any data yet.

## Download Tranquility

In the Druid package root, run the following commands:

```
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.2.tgz -o tranquility-distribution-0.8.2.tgz
tar -xzf tranquility-distribution-0.8.2.tgz
mv tranquility-distribution-0.8.2 tranquility
```

The startup scripts for the tutorial will expect the contents of the Tranquility tarball to be located at `tranquility` under the druid-#{DRUIDVERSION} package root.

## Enable Tranquility Server

- In your `quickstart/conf-quickstart/quickstart.conf`, uncomment the `tranquility-server` line.
- Stop your *bin/supervise* command (CTRL-C) and then restart it by again running `bin/supervise -c quickstart/conf-quickstart/quickstart.conf`.

As part of the output of *supervise* you should see something like:

```
Running command[tranquility-server], logging to[/stage/druid-{DRUIDVERSION}/var/sv/tranquility-server.log]: tranquility/bin/tranquility server -configFile quickstart/conf-quickstart/tranquility/server.json -Ddruid.extensions.loadList=[]
```

You can check the log file in `var/sv/tranquility-server.log` to confirm that the server is starting up properly.

## Send data

Let's send the sample Wikipedia edits data to Tranquility:

```
tar -xz quickstart/wikipedia-2016-06-27-sampled.json.gz | curl -XPOST -H'Content-Type: application/json' --data-binary @- http://localhost:8200/v1/post/tutorial-tranquility-server
```

Which will print something like:

```
{"result":{"received":24433,"sent":24433}}
```

This indicates that the HTTP server received 24,433 events from you, and sent 24,433 to Druid. This
command may generate a "connection refused" error if you run it too quickly after enabling Tranquility
Server, which means the server has not yet started up. It should start up within a few seconds. The command
may also take a few seconds to finish the first time you run it, during which time Druid resources are being
allocated to the ingestion task. Subsequent POSTs will complete quickly once this is done.

Once the data is sent to Druid, you can immediately query it.

## Querying your data

Please follow the [query tutorial](../tutorial/query.html) to run some example queries on the newly loaded data.

## Further reading

For more information on Tranquility, please see [the Tranquility documentation](https://github.com/druid-io/tranquility).
