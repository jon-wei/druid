---
layout: doc_page
title: "Performance Tuning"
---

This document discusses configuration properties and cluster architecture considerations that are relevant for performance tuning of an Apache Druid (incubating) deployment.

# Processing Threads, Buffers and Direct Memory

## Processing Threads

Brokers, Historicals, and Peons that are ingesting realtime data can accept queries. 

The `druid.processing.numThreads` configuration controls the size of the processing thread pool used for computing query results. The size of this pool limits how many queries can be concurrently processed.

For Brokers and Historicals, this property should generally be set to (number of cores - 1): a smaller value can result in CPU underutilization, while going over the number of cores can result in unnecessary CPU contention.

For Peons, the thread pool size can be lower, 1 or 2 processing threads are often enough, as the realtime ingestion tasks tend to hold much less queryable data compared to Historical processes.

## Processing Buffers

`druid.processing.buffer.sizeBytes` is a closely related property that controls the size of the off-heap buffers allocated to the processing threads. One buffer is allocated for each processing thread; the default size of 1GB is typically sufficient.

### GroupBy Merging Buffers

If you plan to issue GroupBy queries, `druid.processing.numMergeBuffers` is an important configuration property. GroupBy queries require an additional set of off-heap buffers

## Other Direct Memory Usage

# Connection Pool Sizing

# Heap Sizing

# Worker Configuration

## Kafka Ingestion

## Hadoop Ingestion

## Parallel Native Ingestion

# Segment Management


