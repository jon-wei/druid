---
layout: doc_page
title: "Performance Tuning"
---

This document provides basic guidelines for configuration properties and cluster architecture considerations that are relevant for performance tuning of an Apache Druid (incubating) deployment.

The recommendations and tuning principles are organized by process type.

# Coordinator

## Total Memory Usage

# Overlord

# Historical

# MiddleManager

## Task Configuration

### Kafka Ingestion

### Hadoop Ingestion

### Parallel Native Ingestion

# Broker

# Router


# Processing Threads and Buffers

## Processing Threads

Brokers, Historicals, and realtime-ingestion Peons can accept queries. 

The `druid.processing.numThreads` configuration controls the size of the processing thread pool used for computing query results. The size of this pool limits how many queries can be concurrently processed.

For Brokers and Historicals, this property should generally be set to (number of cores - 1): a smaller value can result in CPU underutilization, while going over the number of cores can result in unnecessary CPU contention.

For Peons, the thread pool size can be lower. 1 or 2 processing threads are often enough, as the realtime ingestion tasks tend to hold much less queryable data than Historical processes.

## Processing Buffers

`druid.processing.buffer.sizeBytes` is a closely related property that controls the size of the off-heap buffers allocated to the processing threads. 

One buffer is allocated for each processing thread; the default size of 1GB is typically sufficient.

The TopN and GroupBy queries use these buffers to store intermediate computed results. As the buffer size increases, more data can be processed in a single pass.

## GroupBy Merging Buffers

If you plan to issue GroupBy queries, `druid.processing.numMergeBuffers` is an important configuration property. 

GroupBy queries use an additional pool of off-heap buffers for merging query results. These buffers have the same size as the processing buffers described above, set by the `druid.processing.buffer.sizeBytes` property.

Non-nested GroupBy queries require 1 merge buffer per query, while a nested GroupBy query requires 2 merge buffers (regardless of the depth of nesting). 

The number of merge buffers determines the number of GroupBy queries that can be processed concurrently.

# Connection Pool Sizing

Druid has a number of configuration properties related to the sizing of HTTP connection thread pools. 

Since the sizes of the connection thread pools limit the number of queries that can be concurrently processed, these configuration properties are important for performance tuning.

When thinking about how to size connection pools, 

# Heap Sizing




# Segment Management

## Direct Memory Usage

### Segment Decompression

When opening a segment for reading during segment merging or query processing, Druid allocates a 64KB off-heap decompression buffer for each column being read.

Thus, there is additional direct memory overhead of (64KB * number of columns read per segment * number of segments read) when reading segments.

### Segment Merging

In addition to the segment decompression overhead described above, when a set of segments are merged during ingestion, a direct buffer is allocated for every String typed column, for every segment in the set to be merged. 

The size of these buffers are equal to the cardinality of the String column within its segment, times 4 bytes (the buffers store integers).
 
For example, if two segments are being merged, the first segment having a single String column with cardinality 1000, and the second segment having a String column with cardinality 500, the merge step would allocate (1000 + 500) * 4 = 6000 bytes of direct memory. 
 
These buffers are used for merging the value dictionaries of the String column across segments. These "dictionary merging buffers" are independent of the "merge buffers" configured by `druid.processing.numMergeBuffers`.


