package com.github.vivekkothari.persister;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author vivek.kothari on 16/06/16.
 */
@Slf4j
public class ESBulkProcessor
        implements Managed, Supplier<BulkProcessor> {

    private final ESRiverConfiguration esConfig;
    private BulkProcessor bulkProcessor;
    private TransportClient client;

    public ESBulkProcessor(final ESRiverConfiguration esConfig) {
        this.esConfig = esConfig;
    }

    @Override
    public void start() throws Exception {
        log.info("Starting Elasticsearch Client Connection...");
        final Settings settings = ImmutableSettings.builder()
                                                   .put("cluster.name", esConfig.getClusterName())
                                                   .build();

        client = new TransportClient(settings);
        for (final String host : esConfig.getHosts()) {
            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host.trim(), 9300)));
            log.info("Added Elasticsearch Node : {}", host);
        }
        log.info("Started Elasticsearch Client Connection...");
        log.info("Starting bulk processor..");
        bulkProcessor = BulkProcessor.builder(client, new ESBulkListener())
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
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        log.info("Stopped bulk processor..");
    }

    public TransportClient getClient() {
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
        public void afterBulk(final long executionId, final BulkRequest request, final BulkResponse response) {
            log.info("finished bulk operation {} which took {} and # items processed are {}", executionId, response.getTookInMillis(), response.getItems().length);
            if (response.hasFailures()) {
                log.error("Error in operation {}, message: {}", executionId, response.buildFailureMessage());
            }
        }

        @Override
        public void afterBulk(final long executionId, final BulkRequest request, final Throwable failure) {
            log.error("Error in operation {}", executionId, failure);
        }
    }
}
