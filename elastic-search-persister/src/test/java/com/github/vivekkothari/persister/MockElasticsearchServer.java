package com.github.vivekkothari.persister;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class MockElasticsearchServer {
    private final Node node;
    private String DATA_DIRECTORY = "target/" + UUID.randomUUID()
                                                    .toString() + "/elasticsearch-data";
    private List<String> cleanUpIndexes;

    public MockElasticsearchServer(String clusterName, List<String> cleanUpIndexes) {
        this.cleanUpIndexes = cleanUpIndexes;
        Settings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                                                                  .put("http.enabled", "false")
                                                                  .put("path.home", DATA_DIRECTORY);
        node = nodeBuilder().local(true)
                            .settings(elasticsearchSettings.build())
                            .clusterName(clusterName)
                            .data(true)
                            .client(false)
                            .node();
    }

    public void refresh(final String index) {
        node.client()
            .admin()
            .indices()
            .refresh(new RefreshRequest().indices(index))
            .actionGet();
    }

    public Client getClient() {
        return node.client();
    }

    public void shutdown() throws IOException {
        if (cleanUpIndexes != null && cleanUpIndexes.size() > 0) {
            for (String index : cleanUpIndexes) {
                node.client()
                    .admin()
                    .indices()
                    .delete(new DeleteIndexRequest(index));
            }
        }
        node.close();
        deleteDataDirectory();
    }

    public void deleteIndex(String index) {
        node.client()
            .admin()
            .indices()
            .delete(new DeleteIndexRequest(index));
    }

    private void deleteDataDirectory() throws IOException {
        System.out.println("Deleting directory ");
        FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    }
}
