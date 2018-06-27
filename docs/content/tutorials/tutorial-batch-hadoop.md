---
layout: doc_page
---

# Tutorial: Load your own batch data using Hadoop

This tutorial shows you how to load your own data files into Druid using a remote Hadoop cluster.

For this tutorial, we'll assume that you've already completed the previous [batch ingestion tutorial](tutorial-batch.html).
using Druid's native batch ingestion system.

## Writing an Ingestion Spec

The remote Hadoop indexing task uses a different ingestion spec format from the native batch indexing task in the previous tutorial.



