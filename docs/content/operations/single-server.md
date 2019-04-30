---
layout: doc_page
title: "Single Server Deployments"
---

# Example configurations
------------

Micro: 4 CPU, 16GB RAM

Coordinator: 128MB heap
Overlord: 128MB heap
Broker: 256MB heap, 500MB dir 
Hist: 256MB Heap, ~1GB direct, 300GB segment cache
MM: 64MB heap, 2 workers * (500MB direct + 1GB heap)
Router: 128MB heap


128 + 128 + 256 + 500 + 256 + 1000 + 64 + 3000 + 128
------------

Small: 8 CPU, 64GB RAM
------------

Medium: 16 CPU, 128GB RAM
------------

LARGE: 32 CPU, 256 GB RAM


