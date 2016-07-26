package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 25/07/16.
 */
public class TableNameFilterTest {

    @Test(expected = NullPointerException.class)
    public void shouldProcess_error() throws Exception {
        TableNameFilter filter = new TableNameFilter(null);
    }

    @Test
    public void shouldProcess() throws Exception {
        TableNameFilter filter = new TableNameFilter("Table");
        MessageValue messageValue = new MessageValue();
        messageValue.setTable("Table");
        Assert.assertEquals(true, filter.shouldProcess(messageValue));

        messageValue.setTable("InvalidTable");
        Assert.assertEquals(false, filter.shouldProcess(messageValue));
    }

}