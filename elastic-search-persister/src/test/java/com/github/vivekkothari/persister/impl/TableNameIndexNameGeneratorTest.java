package com.github.vivekkothari.persister.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class TableNameIndexNameGeneratorTest {

    @Test
    public void indexName() throws Exception {
        final MessageValue value = new MessageValue();
        value.setDatabase("db");
        value.setTable("Table");
        TableNameIndexNameGenerator indexNameGenerator = new TableNameIndexNameGenerator();
        final String indexName = indexNameGenerator.indexName(value, new Date());
        Assert.assertEquals("table", indexName);
    }

}