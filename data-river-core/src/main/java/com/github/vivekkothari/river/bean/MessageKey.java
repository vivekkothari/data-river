package com.github.vivekkothari.river.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * @author vivek.kothari on 15/05/16.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageKey {

    private String database;

    private String table;

}
