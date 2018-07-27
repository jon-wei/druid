---
layout: doc_page
---

# Tutorial: Load streaming data with Tranquility

## Getting started

This tutorial shows you how to load streaming data into Druid using Tranquility Server.

[Tranquility Server](https://github.com/druid-io/tranquility/blob/master/docs/server.md) allows a stream of data to be pushed into Druid over HTTP.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [single-machine quickstart](quickstart.html) and have it running on your local machine. You
don't need to have loaded any data yet.

## Download Tranquility

In the Druid package root, run the following commands:

```
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.2.tgz -o tranquility-distribution-0.8.2.tgz
tar -xzf tranquility-distribution-0.8.2.tgz
```

The startup scripts for the tutorial will expect the contents of the Tranquility tarball to be located at `tranquility` under the druid-#{DRUIDVERSION} package root.

## Enable Tranquility Server

- In your `quickstart/conf-quickstart/quickstart.conf`, uncomment the `tranquility-server` line.
- Stop your *bin/supervise* command (CTRL-C) and then restart it by
again running `bin/supervise -c quickstart/conf-quickstart/quickstart.conf`.

As part of the output of *supervise* you should see something like:

```
Running command[tranquility-server], logging to[/stage/druid-{DRUIDVERSION}/var/sv/tranquility-server.log]: tranquility/bin/tranquility server -configFile quickstart/conf-quickstart/tranquility/server.json
```

You can check the log file in `var/sv/tranquility-server.log` to confirm that the server is
starting up properly.

