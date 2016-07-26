package com.github.vivekkothari.persister;

import com.codahale.metrics.health.HealthCheck;
import com.github.vivekkothari.river.config.RiverConfiguration;
import com.github.vivekkothari.river.service.IPersister;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import io.dropwizard.util.SizeUnit;
import io.dropwizard.validation.MaxDuration;
import io.dropwizard.validation.MaxSize;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.MinSize;
import lombok.Data;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author vivek.kothari on 26/07/16.
 */
@Data
public class ESRiverConfiguration
        extends RiverConfiguration {

    @NotEmpty
    private String clusterName;

    @Range(min = 0, max = 100000)
    private int bulkActions = 10000;

    @MinSize(value = 1, unit = SizeUnit.MEGABYTES)
    @MaxSize(value = 1, unit = SizeUnit.GIGABYTES)
    private Size bulkSize = Size.megabytes(128);

    @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    @MaxDuration(value = 1, unit = TimeUnit.MINUTES)
    private Duration flushInterval = Duration.seconds(30);

    @NotEmpty
    private List<String> hosts;

    @Override
    protected IPersister doBuild(final Environment environment) {
        ESBulkProcessor bulkProcessor = new ESBulkProcessor(this);
        environment.lifecycle()
                   .manage(bulkProcessor);
        environment.healthChecks()
                   .register("es-health", new HealthCheck() {

                       @Override
                       protected Result check() throws Exception {
                           ListenableActionFuture<ClusterHealthResponse> execute = bulkProcessor.getClient()
                                                                                                .admin()
                                                                                                .cluster()
                                                                                                .prepareHealth()
                                                                                                .execute();
                           ClusterHealthResponse healthResponse = execute.actionGet();
                           ClusterHealthStatus status = healthResponse.getStatus();

                           String clusterName = healthResponse.getClusterName();
                           if (status.value() > ClusterHealthStatus.YELLOW.value()) {
                               return Result.unhealthy("ES cluster " + clusterName + " is unhealthy.");
                           }
                           return Result.healthy("ES cluster " + clusterName + " is healthy.");
                       }
                   });
        return new ElasticSearchPersister(bulkProcessor);
    }
}
