package com.github.vivekkothari.persister;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IBackFiller;
import com.github.vivekkothari.river.service.TransformerFactory;
import com.github.vivekkothari.river.service.impl.NoopEnricher;
import com.github.vivekkothari.river.service.impl.PassAllFilter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Date;

import static org.mockito.Mockito.*;

/**
 * @author vivek.kothari on 27/07/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchPersisterTest {

    private ElasticSearchPersister persister;

    @Mock
    private BulkProcessor bulkProcessor;

    @Mock
    private IndexNameGenerator indexNameGenerator;

    static IndexRequest indexRequestEq(IndexRequest expected) {
        return argThat(new IndexRequestMatcher(expected));
    }

    static DeleteRequest deleteRequestEq(DeleteRequest expected) {
        return argThat(new DeleteRequestMatcher(expected));
    }

    @Before
    public void before() {
        TransformerFactory.builder()
                          .filter("river1", new PassAllFilter())
                          .enricher("river1", new NoopEnricher())
                          .backFiller("river1", Mockito.mock(IBackFiller.class))
                          .build();
    }

    @After
    public void after() {
        TransformerFactory.INSTANCE = null;
    }

    @Test
    public void persist_insert() throws Exception {
        persister = new ElasticSearchPersister(() -> bulkProcessor, ImmutableMap.of("river1", indexNameGenerator));
        final MessageValue value = new MessageValue();

        when(indexNameGenerator.indexName(Mockito.any(), Mockito.any())).thenReturn("index");

        value.setData(ImmutableMap.of("id", "1234", "created_at", new Date()));
        value.setTable("table");
        value.setType(MessageValue.Type.insert);
        persister.persist(value, "river1");
        final IndexRequest indexRequest = new IndexRequest("index", "table", "1234").source(value.getData());
        verify(bulkProcessor, times(1)).add(indexRequestEq(indexRequest));
    }

    @Test
    public void persist_update() throws Exception {
        persister = new ElasticSearchPersister(() -> bulkProcessor, ImmutableMap.of("river1", indexNameGenerator));
        final MessageValue value = new MessageValue();

        when(indexNameGenerator.indexName(Mockito.any(), Mockito.any())).thenReturn("index");

        value.setData(ImmutableMap.of("id", "1234", "created_at", new Date()));
        value.setTable("table");
        value.setType(MessageValue.Type.update);
        persister.persist(value, "river1");
        final IndexRequest indexRequest = new IndexRequest("index", "table", "1234").source(value.getData());
        verify(bulkProcessor, times(1)).add(indexRequestEq(indexRequest));
    }

    @Test
    public void persist_delete() throws Exception {
        persister = new ElasticSearchPersister(() -> bulkProcessor, ImmutableMap.of("river1", indexNameGenerator));
        final MessageValue value = new MessageValue();

        when(indexNameGenerator.indexName(Mockito.any(), Mockito.any())).thenReturn("index");

        value.setData(ImmutableMap.of("id", "1234", "created_at", new Date()));
        value.setTable("table");
        value.setType(MessageValue.Type.delete);
        persister.persist(value, "river1");
        final DeleteRequest deleteRequest = new DeleteRequest("index", "table", "1234");

        verify(bulkProcessor, times(1)).add(deleteRequestEq(deleteRequest));
    }

    static class IndexRequestMatcher
            extends ArgumentMatcher<IndexRequest> {

        private final IndexRequest indexRequest;

        IndexRequestMatcher(final IndexRequest indexRequest) {
            this.indexRequest = indexRequest;
        }

        @Override
        public boolean matches(final Object argument) {
            return EqualsBuilder.reflectionEquals(indexRequest, argument);
        }
    }

    static class DeleteRequestMatcher
            extends ArgumentMatcher<DeleteRequest> {

        private final DeleteRequest deleteRequest;

        DeleteRequestMatcher(final DeleteRequest deleteRequest) {
            this.deleteRequest = deleteRequest;
        }

        @Override
        public boolean matches(final Object argument) {
            return EqualsBuilder.reflectionEquals(deleteRequest, argument);
        }
    }

}