package com.github.vivekkothari.persister;

import static com.github.vivekkothari.river.service.IEnricher.dtf;

import com.datasift.dropwizard.kafka.serializer.JacksonDecoder;
import com.github.vivekkothari.persister.health.ESHealthCheck;
import com.github.vivekkothari.persister.impl.DailyIndexNameGenerator;
import com.github.vivekkothari.river.bean.MessageKey;
import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.config.RiverConfiguration;
import com.github.vivekkothari.river.processor.DBStreamProcessor;
import com.github.vivekkothari.river.service.IPersister;
import com.github.vivekkothari.river.service.IRiverService;
import com.github.vivekkothari.river.service.TransformerFactory;
import com.github.vivekkothari.river.service.impl.RiverServiceDateDecorator;
import com.github.vivekkothari.river.service.impl.RiverServiceImpl;
import com.github.vivekkothari.river.task.BackfillTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.DataSize;
import io.dropwizard.util.Duration;
import io.dropwizard.util.SizeUnit;
import io.dropwizard.validation.MaxDuration;
import io.dropwizard.validation.MaxSize;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.MinSize;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotEmpty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.validator.constraints.Range;

/**
 * @author vivek.kothari on 26/07/16.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ESRiverConfiguration
    extends RiverConfiguration {

  @NotEmpty
  private String clusterName;

  @Range(min = 0, max = 100000)
  private int bulkActions = 10000;

  @MinSize(value = 1, unit = SizeUnit.MEGABYTES)
  @MaxSize(value = 1, unit = SizeUnit.GIGABYTES)
  private DataSize bulkSize = DataSize.megabytes(128);

  @MinDuration(value = 1)
  @MaxDuration(value = 1, unit = TimeUnit.MINUTES)
  private Duration flushInterval = Duration.seconds(30);

  @NotEmpty
  private List<String> hosts;

  public void build(final Environment environment) {
    Preconditions.checkState(TransformerFactory.INSTANCE != null,
        "Transformer factory must be initialized before building this");
    var indexNameGenerators = ImmutableMap.<String, IndexNameGenerator>builder();
    final var value = new DailyIndexNameGenerator();
    TransformerFactory.INSTANCE.getRiverTypes()
        .forEach(key -> indexNameGenerators.put(key, value));
    build(environment, indexNameGenerators.build());
  }

  public void build(final Environment environment,
      Map<String, IndexNameGenerator> indexNameGenerators) {
    Preconditions.checkState(TransformerFactory.INSTANCE != null,
        "Transformer factory must be initialized before building this");
    for (final var riverType : TransformerFactory.INSTANCE.getRiverTypes()) {
      Preconditions.checkState(indexNameGenerators.containsKey(riverType),
          "No indexNameGenerator for " + riverType);
    }
    final var objectMapper = environment.getObjectMapper();
    objectMapper.setDateFormat(dtf.get());

    final var iPersister = doBuild(environment, indexNameGenerators);
    final IRiverService riverService = new RiverServiceImpl(iPersister);

    final IRiverService riverServiceDecorator = new RiverServiceDateDecorator(riverService,
        getDateFields());

    environment.admin()
        .addTask(new BackfillTask(riverService));

    final var streamProcessor = new DBStreamProcessor(riverServiceDecorator);

    final var keyDecoder = new JacksonDecoder<>(objectMapper,
        MessageKey.class);
    final var valueDecoder = new JacksonDecoder<>(objectMapper,
        MessageValue.class);
    getKafkaConsumerFactory().processWith(keyDecoder, valueDecoder, streamProcessor)
        .build(environment);
  }

  protected IPersister doBuild(final Environment environment,
      Map<String, IndexNameGenerator> indexNameGenerators) {
    var bulkProcessor = new ESBulkProcessor(this);
    environment.lifecycle()
        .manage(bulkProcessor);
    environment.healthChecks()
        .register("es-health", new ESHealthCheck(bulkProcessor));
    return new ElasticSearchPersister(bulkProcessor, indexNameGenerators);
  }
}
