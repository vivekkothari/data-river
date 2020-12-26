# data-river [![Build Status](https://travis-ci.org/vivekkothari/data-river.svg?branch=master)](https://travis-ci.org/vivekkothari/data-river) [![Coverage Status](https://coveralls.io/repos/github/vivekkothari/data-river/badge.svg?branch=master)](https://coveralls.io/github/vivekkothari/data-river?branch=master)

Replicates data from mysql to any datastore of your choice (relies on maxwell)

### Maven Dependency

* Use the following maven dependency:

```
<dependency>
    <groupId>com.github.vivekkothari</groupId>
    <artifactId>elastic-search-persister</artifactId>
    <version>1.2.1</version>
</dependency>
```

Imagine you have 2 tables in MySql, `Table1` and `Table2`. You would have to
configure [maxwell](http://maxwells-daemon.io). Once maxwell is properly configured, lets say you
want to persist changes in the above tables in [Elastic-Search](https://www.elastic.co). We would
create 2 rivers, `table1_river` and `table2_river`. Then for each of these `riverType`, provide
implementation of `IFilter`, `IEnricher` and `IBackFiller` and build a
[`TransformerFactory`](https://github.com/vivekkothari/data-river/blob/master/data-river-core/src/main/java/com/github/vivekkothari/river/service/TransformerFactory.java)

Incoming kafka message goes through following 3 steps:

1.
Filtering: [`IFilter`](https://github.com/vivekkothari/data-river/blob/master/data-river-core/src/main/java/com/github/vivekkothari/river/service/IFilter.java)
governs whether the incoming message should be processed or not.
2.
Enrichment: [`IEnricher`](https://github.com/vivekkothari/data-river/blob/master/data-river-core/src/main/java/com/github/vivekkothari/river/service/IEnricher.java)
provides a way to enrich the incoming message. (think of joining the row with some other row)
3.
Persistence: [`IPersister`](https://github.com/vivekkothari/data-river/blob/master/data-river-core/src/main/java/com/github/vivekkothari/river/service/IPersister.java)
persists the message in your desired data store.

There is also and admin task on the admin port of your dropwizard application which can be used to
backfill the data. Example

```
http://localhost:8080/admin/backfill?startDate=2016-01-01T00:00:00&endDate=2016-01-10T00:00:00&riverType=river1
```

Add following code in the `run` method of you dropwizard application.
Add `com.github.vivekkothari.persister.ESRiverConfiguration` in your `Configuration` class Configure
appropriately (like Elasticsearch hosts, bulk index configs etc.) then call following method.

```
configuration.getEsRiverConfiguration().build(environment);
```

The `IPersister` can be extended and you can add a new data store to which the messages can be
stored.
