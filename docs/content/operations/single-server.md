---
layout: doc_page
title: "Single Server Deployments"
---

# Example configurations


Micro-Tutorial: 4 CPU, 16GB RAM
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
Broker: 4GB heap, 4GB dir
Hist: 4GB heap, 4GB dir
MM: 64MB heap, 4 workers * (1GB direct + 2GB heap)
Rotuer: 512MB heap

1000 + 1000 + 8000 + 8000 + 64 + 12000 + 512 = 30576 (leave some room for mmap segments)



Medium: 32 CPU, 256GB RAM
------------

Coordinator: 2GB heap
Overlord: 2GB heap
Broker: 24GB heap, 12GB dir
Hist: 24GB heap, 12GB dir
MM: 128MB heap, 12 workers * (1GB direct + 2GB heap)
Rotuer: 2GB heap

2000 + 2000 + 36000 + 36000 + 128 + 36000 + 2000 = 114128GB (128GB for mmap)



Large: 64 CPU, 512 GB RAM
------------

Coordinator: 4GB heap
Overlord: 4GB heap
Broker: 48GB heap, 24GB dir
Hist: 48GB heap, 24GB dir
MM: 256MB heap, 16 workers * (1GB direct + 2GB heap)
Rotuer: 4GB heap

4000 + 4000 + 72000 + 72000 + 256 + 48000 + 4000 = 204256GB (~300GB for mmap)