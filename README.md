# data-river
Replicates data from mysql to any datastore of your choice (relies on maxwell)

### Maven Dependency
* Use the following maven dependency:
```
<dependency>
    <groupId>com.github.vivekkothari</groupId>
    <artifactId>elastic-search-persister</artifactId>
    <version>1.2</version>
</dependency>
```

Add following code in the `run` method of you dropwizard application.
Add `com.github.vivekkothari.persister.ESRiverConfiguration` in your `Configuration` class
Configure appropriately (like Elasticsearch hosts, bulk index configs etc.) then call following method.
```
configuration.getEsRiverConfiguration().build(environment);
```

The `IPersister` can be extended and you can add a new data store to which the messages can be stored.
