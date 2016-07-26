package com.github.vivekkothari.persister;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IEnricher;
import com.github.vivekkothari.river.service.IPersister;
import com.github.vivekkothari.river.service.TransformerFactory;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author vivek.kothari on 26/07/16.
 */
public class ElasticSearchPersister
        implements IPersister {

    private final Supplier<BulkProcessor> bulkProcessor;
    private final Map<String, IndexNameGenerator> indexNameGenerators;

    public ElasticSearchPersister(final Supplier<BulkProcessor> bulkProcessor, final Map<String, IndexNameGenerator> indexNameGenerators) {
        this.bulkProcessor = bulkProcessor;
        this.indexNameGenerators = indexNameGenerators;
    }

    @Override
    public void persist(final MessageValue messageValue, final String riverType) {
        final IEnricher enricher = TransformerFactory.INSTANCE.enricher(riverType);
        final String id = enricher.recordId(messageValue);
        final String indexName = indexNameGenerators.get(riverType)
                                                    .indexName(messageValue, enricher.getRecordCreationDate(messageValue));
        final String type = messageValue.getTable()
                                        .toLowerCase();
        switch (messageValue.getType()) {
            case insert:
                bulkProcessor.get()
                             .add(new IndexRequest(indexName, type, id).source(messageValue.getData()));
                break;
            case update:
                bulkProcessor.get()
                             .add(new IndexRequest(indexName, type, id).source(messageValue.getData()));
                break;
            case delete:
                bulkProcessor.get()
                             .add(new DeleteRequest(indexName, type, id));
                break;
        }
    }

}
