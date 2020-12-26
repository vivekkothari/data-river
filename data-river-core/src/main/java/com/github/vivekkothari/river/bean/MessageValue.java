package com.github.vivekkothari.river.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author vivek.kothari on 15/05/16.
 */
@Data
public class MessageValue {

  private String database;
  private String table;
  private Type type;

  @JsonProperty("ts")
  @JsonDeserialize(using = UnixTimestampDeserializer.class)
  private Timestamp timestamp;

  @JsonProperty("xid")
  private long commitId;

  @JsonProperty("commit")
  private boolean committed;

  private Map<String, Object> data;

  private Map<String, Object> old;

  public enum Type {
    insert, update, delete
  }

  @Slf4j
  private static class UnixTimestampDeserializer
      extends JsonDeserializer<Timestamp> {

    @Override
    public Timestamp deserialize(final JsonParser jsonParser,
        final DeserializationContext deserializationContext)
        throws IOException {
      return new Timestamp(jsonParser.getLongValue() * 1000);
    }
  }
}
