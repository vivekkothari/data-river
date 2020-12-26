package com.github.vivekkothari.persister;

import io.dropwizard.lifecycle.Managed;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author vivek.kothari on 16/06/16.
 */
@Slf4j
public class ESBulkProcessor
    implements Managed, Supplier<BulkProcessor> {

  private final ESRiverConfiguration esConfig;
  private BulkProcessor bulkProcessor;
  private RestHighLevelClient client;

  public ESBulkProcessor(final ESRiverConfiguration esConfig) {
    this.esConfig = esConfig;
  }

  @Override
  public void start() {
    log.info("Starting Elasticsearch Client Connection...");

    var httpHosts = esConfig.getHosts().stream()
        .map(host -> new HttpHost(host, 9200, "http"))
        .toArray(HttpHost[]::new);

    client = new RestHighLevelClient(RestClient.builder(httpHosts));

    log.info("Started Elasticsearch Client Connection...");
    log.info("Starting bulk processor..");
    bulkProcessor = BulkProcessor.builder(
        (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
        new ESBulkListener())
        .setBulkActions(esConfig.getBulkActions())
        .setBulkSize(new ByteSizeValue(esConfig.getBulkSize()
            .toBytes(), ByteSizeUnit.BYTES))
        .setFlushInterval(TimeValue.timeValueMillis(esConfig.getFlushInterval()
            .toMilliseconds()))
        .setConcurrentRequests(1)
        .build();
  }

  @Override
  public void stop() throws Exception {
    log.info("Stopping bulk processor..");
    client.close();
    bulkProcessor.close();
    log.info("Stopped bulk processor..");
  }

  public RestHighLevelClient getClient() {
    return Objects.requireNonNull(client);
  }

  @Override
  public BulkProcessor get() {
    return Objects.requireNonNull(bulkProcessor);
  }

  private static class ESBulkListener
      implements BulkProcessor.Listener {

    @Override
    public void beforeBulk(final long executionId, final BulkRequest request) {
      log.info("about to do bulk operation {}", executionId);
    }

    @Override
    public void afterBulk(final long executionId, final BulkRequest request,
        final BulkResponse response) {
      log.info("finished bulk operation {} which took {} and # items processed are {}", executionId,
          response.getIngestTookInMillis(), response.getItems().length);
      if (response.hasFailures()) {
        log.error("Error in operation {}, message: {}", executionId,
            response.buildFailureMessage());
      }
    }

    @Override
    public void afterBulk(final long executionId, final BulkRequest request,
        final Throwable failure) {
      log.error("Error in operation {}", executionId, failure);
    }
  }
}
