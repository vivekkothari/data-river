package com.github.vivekkothari.river.config;

import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka.serializer.JacksonDecoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vivekkothari.river.bean.MessageKey;
import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.processor.DBStreamProcessor;
import com.github.vivekkothari.river.service.IPersister;
import com.github.vivekkothari.river.service.IRiverService;
import com.github.vivekkothari.river.service.TransformerFactory;
import com.github.vivekkothari.river.service.impl.RiverServiceDateDecorator;
import com.github.vivekkothari.river.service.impl.RiverServiceImpl;
import com.github.vivekkothari.river.task.BackfillTask;
import com.google.common.base.Preconditions;
import io.dropwizard.setup.Environment;
import io.dropwizard.validation.ValidationMethod;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Set;

import static com.github.vivekkothari.river.service.IEnricher.dtf;

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
        final String group = kafkaConsumerFactory.getGroup();
        return StringUtils.isNotBlank(group);
    }

    public void build(final Environment environment) {
        Preconditions.checkState(TransformerFactory.INSTANCE != null, "Transformer factory must be initialized before building this");
        final ObjectMapper objectMapper = environment.getObjectMapper();
        objectMapper.setDateFormat(dtf.get());

        final IPersister iPersister = doBuild(environment);
        final IRiverService riverService = new RiverServiceImpl(iPersister);

        final IRiverService riverServiceDecorator = new RiverServiceDateDecorator(riverService, dateFields);

        environment.admin()
                   .addTask(new BackfillTask(riverService));

        final DBStreamProcessor streamProcessor = new DBStreamProcessor(riverServiceDecorator);

        final JacksonDecoder<MessageKey> keyDecoder = new JacksonDecoder<>(objectMapper, MessageKey.class);
        final JacksonDecoder<MessageValue> valueDecoder = new JacksonDecoder<>(objectMapper, MessageValue.class);
        kafkaConsumerFactory.processWith(keyDecoder, valueDecoder, streamProcessor)
                            .build(environment);

    }

    protected abstract IPersister doBuild(final Environment environment);

}
