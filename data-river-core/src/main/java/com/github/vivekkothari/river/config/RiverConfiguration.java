package com.github.vivekkothari.river.config;

import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import io.dropwizard.validation.ValidationMethod;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author vivek.kothari on 25/07/16.
 */
@Data
public abstract class RiverConfiguration {

  @Valid
  @NotNull
  private KafkaConsumerFactory kafkaConsumerFactory;

  @NotEmpty
  private Set<String> dateFields;

  @ValidationMethod(message = "consumerGroup is not valid")
  public boolean validateRiverGroup() {
    final var group = kafkaConsumerFactory.getGroup();
    return StringUtils.isNotBlank(group);
  }

}
