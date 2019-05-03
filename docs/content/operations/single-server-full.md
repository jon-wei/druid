---
layout: doc_page
title: "Single Server Deployments"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->
  
# Single Server Deployments

Druid includes a set of reference configurations and launch scripts for single-machine deployments:

- `micro-quickstart`
- `small`
- `medium`
- `large`
- `xlarge`

The `micro-quickstart` is sized for small machines like laptops and is intended for quick evaluation use-cases.

The other configurations are intended for general use single-machine deployments. They are sized for hardware roughly based on Amazon's i3 series of EC2 instances.


## Single Server Reference Configurations

Micro-Quickstart: 4 CPU, 16GB RAM
------------
Launch command: `bin/start-micro-quickstart`
Configuration directory: `conf/druid/single-server/micro-quickstart`

Coordinator: 128MB heap
Overlord: 128MB heap
Broker: 512MB heap, 400MB direct memory
Hist: 512MB Heap, ~1GB direct
MM: 64MB heap, 2 workers * (500MB direct + 1GB heap)
Router: 128MB heap


Small: 8 CPU, 64GB RAM (i3.2xlarge)
------------
Coordinator: 3GB heap
Overlord: 3GB heap
Broker: 4GB heap, 2GB dir
Hist: 4GB heap, 5.5GB dir
MM: 128MB heap, 3 workers * (400MB direct + 1GB heap)
Router: 512MB heap

256MB query cache


Medium: 16 CPU, 128GB RAM (i3.4xlarge)
------------
Coordinator: 6GB heap
Overlord: 6GB heap
Broker: 8GB heap, 2.5GB dir
Hist: 8GB heap, 10.5GB dir
MM: 256MB heap, 4 workers * (400MB direct + 1GB heap)
Rotuer: 512MB heap

1000 + 1000 + 16000 + 16000 + 64 + 18000 + 512 = 52576

12 concurrent queries
12 processing threads at broker
1 processing thread per peon
12 processing threads at historical


Large: 32 CPU, 256GB RAM (i3.8xlarge)
------------
Coordinator: 12GB heap
Overlord: 12GB heap
Broker: 16GB heap, 5GB dir
Hist: 16GB heap, 20.5GB dir
MM: 256MB heap, 8 workers * (400MB direct + 1GB heap)
Router: 1GB heap
1GB cache

2000 + 2000 + 36000 + 36000 + 128 + 36000 + 2000 = 114128GB (128GB for mmap)

24 concurrent queries
24 processing threads at broker
1 processing thread per peon
24 processing threads at historical


X-Large: 64 CPU, 512GB RAM (i3.16xlarge)
------------
Coordinator: 18GB heap
Overlord: 18GB heap
Broker: 24GB heap, 9GB dir
Hist: 24GB heap, 40.5GB dir
MM: 256MB heap, 16 workers * (400MB direct + 1GB heap)
Router: 1GB heap
2gb cache

4000 + 4000 + 72000 + 72000 + 256 + 48000 + 4000 = 204256GB (~300GB for mmap)

48 concurrent queries
48 processing threads at broker
1 processing thread per peon
48 processing threads at historical

