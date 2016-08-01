package com.github.vivekkothari.persister.health;

import com.codahale.metrics.health.HealthCheck;
import com.github.vivekkothari.persister.ESBulkProcessor;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;

/**
 * @author vivek.kothari on 28/07/16.
 */
public class ESHealthCheck
        extends HealthCheck {

    private final ESBulkProcessor bulkProcessor;

    public ESHealthCheck(final ESBulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

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
}
