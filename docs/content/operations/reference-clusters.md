---
layout: doc_page
title: "Reference Cluster Architectures"
---

# Reference Cluster architectures

# Small nodes

data: m5d.large

  {
    "serviceType": "historical",
    "instanceType": "m5d.large",
    "properties": {
      "druid.server.maxSize": 50000000000,
      "druid.server.http.numThreads": 10,
      "druid.segmentCache.locations": "[{\"path\":\"/mnt/var/druid/segment-cache\",\"maxSize\":\"50000000000\"}]",
      "druid.processing.buffer.sizeBytes": 250000000,
      "druid.processing.numMergeBuffers": 2,
      "druid.processing.numThreads": 2,
      "druid.cache.sizeInBytes": 100000000
    },
    "jvmConfig": [
      "-Xms1500m",
      "-Xmx1500m",
      "-XX:MaxDirectMemorySize=3g"
    ]
  },
 
    {
      "serviceType": "middleManager",
      "instanceType": "m5d.large",
      "properties": {
        "druid.indexer.runner.javaOpts": "-server -Xmx1g -XX:MaxDirectMemorySize=3g -Duser.timezone=UTC -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mnt/tmp/druid-peon.hprof -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager",
        "druid.indexer.fork.property.druid.processing.buffer.sizeBytes": 100000000,
        "druid.indexer.fork.property.druid.processing.numMergeBuffers": 2,
        "druid.indexer.fork.property.druid.processing.numThreads": 2,
        "druid.indexer.fork.property.druid.server.http.numThreads": 10,
        "druid.worker.capacity": 2
      },
      "jvmConfig": [
        "-Xms128m",
        "-Xmx128m"
      ]
    },



master: t2.small
  {
    "serviceType": "coordinator",
    "instanceType": "t2.small",
    "jvmConfig": [
      "-Xms448m",
      "-Xmx448m"
    ]
  },

  {
    "serviceType": "overlord",
    "instanceType": "t2.small",
    "jvmConfig": [
      "-Xms448m",
      "-Xmx448m"
    ]
  },


query: m5d.large

  {
    "serviceType": "broker",
    "instanceType": "m5d.large",
    "properties": {
      "druid.processing.numMergeBuffers": 2,
      "druid.processing.numThreads": 2,
      "druid.broker.http.numConnections": 15,
      "druid.processing.buffer.sizeBytes": 500000000,
      "druid.server.http.numThreads": 15
    },
    "jvmConfig": [
      "-Xms3500m",
      "-Xmx3500m",
      "-XX:MaxDirectMemorySize=3500m"
    ]
  },
    {
      "serviceType": "router",
      "instanceType": "m5d.large",
      "properties": {
        "druid.router.http.numConnections": 15,
        "druid.router.http.numMaxThreads": 15,
        "druid.server.http.numThreads": 15
      },
      "jvmConfig": [
        "-Xms850m",
        "-Xmx850m"
      ]
    },

# Medium nodes

data: i3.2xlarge

  {
    "serviceType": "historical",
    "instanceType": "i3.2xlarge",
    "properties": {
      "druid.server.maxSize": 1600000000000,
      "druid.server.http.numThreads": 20,
      "druid.segmentCache.locations": "[{\"path\":\"/mnt/var/druid/segment-cache\",\"maxSize\":\"1600000000000\"}]",
      "druid.processing.buffer.sizeBytes": 500000000,
      "druid.processing.numMergeBuffers": 2,
      "druid.processing.numThreads": 7,
      "druid.cache.sizeInBytes": 500000000
    },
    "jvmConfig": [
      "-Xms6g",
      "-Xmx6g",
      "-XX:MaxDirectMemorySize=24g"
    ]
  },


    {
    "serviceType": "middleManager",
    "instanceType": "i3.2xlarge",
    "properties": {
      "druid.indexer.runner.javaOpts": "-server -Xmx5g -XX:MaxDirectMemorySize=24g -Duser.timezone=UTC -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mnt/tmp/druid-peon.hprof -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager",
      "druid.indexer.fork.property.druid.processing.buffer.sizeBytes": 300000000,
      "druid.indexer.fork.property.druid.processing.numMergeBuffers": 2,
      "druid.indexer.fork.property.druid.processing.numThreads": 2,
      "druid.indexer.fork.property.druid.server.http.numThreads": 20,
      "druid.worker.capacity": 3
    },
    "jvmConfig": [
      "-Xms128m",
      "-Xmx128m"
    ]
  },
  
master: m5.large

  {
    "serviceType": "coordinator",
    "instanceType": "m5.large",
    "jvmConfig": [
      "-Xms3500m",
      "-Xmx3500m"
    ]
  },
  
    {
      "serviceType": "overlord",
      "instanceType": "m5.large",
      "jvmConfig": [
        "-Xms2000m",
        "-Xmx2000m"
      ]
    }

query: m5d.xlarge

  {
    "serviceType": "broker",
    "instanceType": "m5d.xlarge",
    "properties": {
      "druid.processing.numMergeBuffers": 3,
      "druid.processing.numThreads": 4,
      "druid.broker.http.numConnections": 20,
      "druid.processing.buffer.sizeBytes": 700000000,
      "druid.server.http.numThreads": 20
    },
    "jvmConfig": [
      "-Xms6500m",
      "-Xmx6500m",
      "-XX:MaxDirectMemorySize=7g"
    ]
  },
    {
      "serviceType": "router",
      "instanceType": "m5d.xlarge",
      "properties": {
        "druid.router.http.numConnections": 20,
        "druid.router.http.numMaxThreads": 20,
        "druid.server.http.numThreads": 20
      },
      "jvmConfig": [
        "-Xms1500m",
        "-Xmx1500m"
      ]
    },

# Large nodes

data: i3.8xlarge

  {
    "serviceType": "historical",
    "instanceType": "i3.8xlarge",
    "properties": {
      "druid.server.maxSize": 6400000000000,
      "druid.server.http.numThreads": 60,
      "druid.segmentCache.locations": "[{\"path\":\"/mnt/var/druid/segment-cache\",\"maxSize\":\"1600000000000\"}, {\"path\":\"/mnt1/var/druid/segment-cache\",\"maxSize\":\"1600000000000\"}, {\"path\":\"/mnt2/var/druid/segment-cache\",\"maxSize\":\"1600000000000\"}, {\"path\":\"/mnt3/var/druid/segment-cache\",\"maxSize\":\"1600000000000\"}]",
      "druid.processing.buffer.sizeBytes": 1000000000,
      "druid.processing.numMergeBuffers": 6,
      "druid.processing.numThreads": 31,
      "druid.cache.sizeInBytes": 2000000000
    },
    "jvmConfig": [
      "-Xms24g",
      "-Xmx24g",
      "-XX:MaxDirectMemorySize=70g"
    ]
  },
  
  {
    "serviceType": "middleManager",
    "instanceType": "i3.8xlarge",
    "properties": {
      "druid.indexer.runner.javaOpts": "-server -Xmx5g -XX:MaxDirectMemorySize=70g -Duser.timezone=UTC -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mnt/tmp/druid-peon.hprof -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager",
      "druid.indexer.fork.property.druid.processing.buffer.sizeBytes": 500000000,
      "druid.indexer.fork.property.druid.processing.numMergeBuffers": 2,
      "druid.indexer.fork.property.druid.processing.numThreads": 2,
      "druid.indexer.fork.property.druid.server.http.numThreads": 25,
      "druid.worker.capacity": 10
    },
    "jvmConfig": [
      "-Xms256m",
      "-Xmx256m"
    ]
  },
  
master: m5.2xlarge

  {
    "serviceType": "coordinator",
    "instanceType": "m4.2xlarge",
    "jvmConfig": [
      "-Xms16g",
      "-Xmx16g"
    ]
  },
  
    {
      "serviceType": "overlord",
      "instanceType": "m4.2xlarge",
      "jvmConfig": [
        "-Xms12g",
        "-Xmx12g"
      ]
    },


query: c5.4xlarge

  {
    "serviceType": "broker",
    "instanceType": "c5.4xlarge",
    "properties": {
      "druid.processing.numMergeBuffers": 4,
      "druid.processing.numThreads": 15,
      "druid.broker.http.numConnections": 20,
      "druid.processing.buffer.sizeBytes": 750000000,
      "druid.server.http.numThreads": 30
    },
    "jvmConfig": [
      "-Xms11g",
      "-Xmx11g",
      "-XX:MaxDirectMemorySize=20g"
    ]
  },
  
    {
      "serviceType": "router",
      "instanceType": "c5.4xlarge",
      "properties": {
        "druid.router.http.numConnections": 20,
        "druid.router.http.numMaxThreads": 40,
        "druid.server.http.numThreads": 40
      },
      "jvmConfig": [
        "-Xms1500m",
        "-Xmx1500m"
      ]
    },

