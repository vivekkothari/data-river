package com.github.vivekkothari.persister.health;

import com.codahale.metrics.health.HealthCheck;
import com.github.vivekkothari.persister.ESBulkProcessor;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

/**
 * @author vivek.kothari on 28/07/16.
 */
public class ESHealthCheck
    extends HealthCheck {

  private final ESBulkProcessor bulkProcessor;

  public ESHealthCheck(final ESBulkProcessor bulkProcessor) {
    this.bulkProcessor = bulkProcessor;
  }

  @SneakyThrows
  @Override
  protected Result check() {
    var status = bulkProcessor.getClient()
        .cluster()
        .health(new ClusterHealthRequest("*"), RequestOptions.DEFAULT)
        .getStatus();
    var clusterName = bulkProcessor.getClient()
        .cluster()
        .health(new ClusterHealthRequest("*"), RequestOptions.DEFAULT)
        .getClusterName();

    if (status.value() > ClusterHealthStatus.YELLOW.value()) {
      return Result.unhealthy("ES cluster " + clusterName + " is unhealthy.");
    }
    return Result.healthy("ES cluster " + clusterName + " is healthy.");
  }
}
