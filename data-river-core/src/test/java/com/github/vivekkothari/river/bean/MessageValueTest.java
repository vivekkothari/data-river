package com.github.vivekkothari.river.bean;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class MessageValueTest {

    private ObjectMapper mapper = Jackson.newObjectMapper();

    @Test
    public void test() throws IOException {
        String json = "{\"database\":\"db\",\"table\":\"Table\",\"type\":\"insert\",\"ts\":1469577600,\"xid\":4477422,\"commit\":true,\"data\":{\"id\":1,\"amount\":1000}}";
        final MessageValue value = mapper.convertValue(mapper.readTree(json), MessageValue.class);
        assertEquals("db", value.getDatabase());
        assertEquals("Table", value.getTable());
        assertEquals(MessageValue.Type.insert, value.getType());
        assertNotNull(value.getData());
        assertNull(value.getOld());
        assertEquals(1469577600000L, value.getTimestamp()
                                          .getTime());
    }

    @Test
    public void test_failure() throws IOException {
        String json = "{\"database\":\"db\",\"table\":\"Table\",\"type\":\"insert\",\"ts\":1469577600,\"xid\":4477422,\"commit\":true,\"data\":{\"id\":1,\"amount\":1000}}";
        final MessageValue value = mapper.convertValue(mapper.readTree(json), MessageValue.class);
        assertEquals("db", value.getDatabase());
        assertEquals("Table", value.getTable());
        assertEquals(MessageValue.Type.insert, value.getType());
        assertNotNull(value.getData());
        assertNull(value.getOld());
        assertEquals(1469577600000L, value.getTimestamp()
                                          .getTime());
    }

}
