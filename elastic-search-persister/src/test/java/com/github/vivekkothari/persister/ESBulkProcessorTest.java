package com.github.vivekkothari.persister;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class ESBulkProcessorTest {

  @Test
  public void test() throws Exception {
    var configuration = new ESRiverConfiguration();
    configuration.setHosts(Lists.newArrayList("localhost"));
    configuration.setClusterName("elasticsearch");
    var processor = new ESBulkProcessor(configuration);
    processor.start();
    processor.get()
        .add(new IndexRequest("index", "type", "id").source(ImmutableMap.of("key", "value")));
    processor.get()
        .flush();
    processor.stop();
  }

}