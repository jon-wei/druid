---
layout: doc_page
title: "Single Server Deployments"
---

# Example configurations


Micro-Quickstart: 4 CPU, 16GB RAM
------------

Coordinator: 128MB heap
Overlord: 128MB heap
Broker: 512MB heap, 500MB dir 
Hist: 512MB Heap, ~1GB direct
MM: 64MB heap, 2 workers * (500MB direct + 1GB heap)
Router: 128MB heap

300GB segment cache

128 + 128 + 512 + 500 + 512 + 1000 + 64 + 3000 + 128 = 5972GB



Small: 8 CPU, 64GB RAM
------------

Coordinator: 1GB heap
Overlord: 1GB heap
Broker: 4GB heap, 6GB dir
Hist: 4GB heap, 6GB dir
MM: 64MB heap, 4 workers * (1GB direct + 1.5GB heap)
Rotuer: 512MB heap

1000 + 1000 + 10000 + 10000 + 64 + 10000 + 512 = 32576 (leave some room for mmap segments)

10 concurrent queries
4 processing threads at broker
1 processing thread per peon
4 processing threads at historical



Medium: 16 CPU, 128GB RAM
------------
Coordinator: 1GB heap
Overlord: 1GB heap
Broker: 8GB heap, 10GB dir
Hist: 8GB heap, 10GB dir
MM: 64MB heap, 8 workers * (1GB direct + 1.5GB heap)
Rotuer: 512MB heap

1000 + 1000 + 16000 + 16000 + 64 + 18000 + 512 = 52576

20 concurrent queries
8 processing threads at broker
1 processing thread per peon
8 processing threads at historical




Large: 32 CPU, 256GB RAM
------------
Coordinator: 2GB heap
Overlord: 2GB heap
Broker: 16GB heap, 20GB dir
Hist: 16GB heap, 20GB dir
MM: 128MB heap, 12 workers * (1GB direct + 1.5GB heap)
Router: 2GB heap

2000 + 2000 + 36000 + 36000 + 128 + 36000 + 2000 = 114128GB (128GB for mmap)




X-Large: 64 CPU, 512GB RAM
------------
Coordinator: 4GB heap
Overlord: 4GB heap
Broker: 48GB heap, 24GB dir
Hist: 48GB heap, 24GB dir
MM: 256MB heap, 16 workers * (1GB direct + 2GB heap)
Rotuer: 4GB heap

4000 + 4000 + 72000 + 72000 + 256 + 48000 + 4000 = 204256GB (~300GB for mmap)