---
layout: doc_page
---

# Tutorial: Querying data

This tutorial will demonstrate various methods of querying data in Druid.

The tutorial assumes that you've already completed one of the 4 ingestion tutorials, as we will be querying the sample Wikipedia edits data.

* [Tutorial: Loading a file](/docs/VERSION/tutorials/tutorial-batch.html)
* [Tutorial: Loading stream data from Kafka](/docs/VERSION/tutorials/tutorial-kafka.html)
* [Tutorial: Loading a file using Hadoop](/docs/VERSION/tutorials/tutorial-batch-hadoop.html)
* [Tutorial: Loading stream data using Tranquility](/docs/VERSION/tutorials/tutorial-tranquility.html)

## Native JSON queries

Druid's native query format is expressed in JSON. We have included a sample native TopN query under `quickstart/wikipedia-top-pages.json`:

```json
{
  "queryType" : "topN",
  "dataSource" : "wikipedia",
  "intervals" : ["2016-06-27/2016-06-28"],
  "granularity" : "all",
  "dimension" : "page",
  "metric" : "count",
  "threshold" : 10,
  "aggregations" : [
    {
      "type" : "count",
      "name" : "count"
    }
  ]
}
```

This query retrieves the 10 Wikipedia pages with the most page edits on 2016-06-27.

Let's submit this query to the Druid broker:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/wikipedia-top-pages.json http://localhost:8082/druid/v2?pretty
```

You should see the following query results:

```json
[ {
  "timestamp" : "2016-06-27T00:00:11.080Z",
  "result" : [ {
    "count" : 29,
    "page" : "Copa América Centenario"
  }, {
    "count" : 16,
    "page" : "User:Cyde/List of candidates for speedy deletion/Subpage"
  }, {
    "count" : 16,
    "page" : "Wikipedia:Administrators' noticeboard/Incidents"
  }, {
    "count" : 15,
    "page" : "2016 Wimbledon Championships – Men's Singles"
  }, {
    "count" : 15,
    "page" : "Wikipedia:Administrator intervention against vandalism"
  }, {
    "count" : 15,
    "page" : "Wikipedia:Vandalismusmeldung"
  }, {
    "count" : 12,
    "page" : "The Winds of Winter (Game of Thrones)"
  }, {
    "count" : 12,
    "page" : "ولاية الجزائر"
  }, {
    "count" : 10,
    "page" : "Copa América"
  }, {
    "count" : 10,
    "page" : "Lionel Messi"
  } ]
} ]
```

## Druid SQL queries

Druid also supports a dialect of SQL for querying. Let's run a SQL query that is equivalent to the native JSON query shown above:

```
SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2016-06-27 00:00:00' AND TIMESTAMP '2016-06-28 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;
```

The SQL queries are submitted as JSON over HTTP. The tutorial package includes an example file that contains the SQL query shown above at `quickstart/wikipedia-top-pages-sql.json`. 

Let's submit that query to the Druid broker:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/wikipedia-top-pages-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```
[
  {
    "page": "Copa América Centenario",
    "Edits": 29
  },
  {
    "page": "User:Cyde/List of candidates for speedy deletion/Subpage",
    "Edits": 16
  },
  {
    "page": "Wikipedia:Administrators' noticeboard/Incidents",
    "Edits": 16
  },
  {
    "page": "2016 Wimbledon Championships – Men's Singles",
    "Edits": 15
  },
  {
    "page": "Wikipedia:Administrator intervention against vandalism",
    "Edits": 15
  },
  {
    "page": "Wikipedia:Vandalismusmeldung",
    "Edits": 15
  },
  {
    "page": "The Winds of Winter (Game of Thrones)",
    "Edits": 12
  },
  {
    "page": "ولاية الجزائر",
    "Edits": 12
  },
  {
    "page": "Copa América",
    "Edits": 10
  },
  {
    "page": "Lionel Messi",
    "Edits": 10
  }
]
```

### dsql client

For convenience, the Druid package includes a SQL command-line client, located at `bin/dsql` from the Druid package root.

Let's now run `bin/dsql`; you should see the following prompt:

```
Welcome to dsql, the command-line client for Druid SQL.
Type "\h" for help.
dsql> 
```

To submit the query, paste it to the `dsql` prompt and press enter:

```
dsql> SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2016-06-27 00:00:00' AND TIMESTAMP '2016-06-28 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;
┌──────────────────────────────────────────────────────────┬───────┐
│ page                                                     │ Edits │
├──────────────────────────────────────────────────────────┼───────┤
│ Copa América Centenario                                  │    29 │
│ User:Cyde/List of candidates for speedy deletion/Subpage │    16 │
│ Wikipedia:Administrators' noticeboard/Incidents          │    16 │
│ 2016 Wimbledon Championships – Men's Singles             │    15 │
│ Wikipedia:Administrator intervention against vandalism   │    15 │
│ Wikipedia:Vandalismusmeldung                             │    15 │
│ The Winds of Winter (Game of Thrones)                    │    12 │
│ ولاية الجزائر                                            │    12 │
│ Copa América                                             │    10 │
│ Lionel Messi                                             │    10 │
└──────────────────────────────────────────────────────────┴───────┘
Retrieved 10 rows in 0.06s.
```

## Further reading

The [Advanced query features tutorial](/docs/VERSION/tutorials/tutorial-query-advanced.html) demonstrates a variety of Druid's query features.

The [Native queries documentation](/docs/VERSION/querying/native.html) has more information on Druid's native JSON queries.

The [Druid SQL documentation](/docs/VERSION/querying/sql.html) has more information on using Druid SQL queries.