package net.pincette.jes.tel;

import static java.lang.System.getProperty;
import static java.util.Optional.ofNullable;
import static net.pincette.config.Util.configValue;

import com.typesafe.config.Config;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.LogRecordProcessor;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.logging.Logger;
import net.pincette.util.Cases;

/**
 * Some OTEL utilities.
 *
 * @author Werner Donné
 */
public class OtelUtil {
  private static final String GRPC = "grpc";
  private static final String HTTP = "http";
  private static final String OS_NAME = "os.name";
  private static final String OS_TYPE = "os.type";
  private static final String OS_VERSION = "os.version";
  private static final String OTLP = "otlp";
  private static final String PROCESS_RUNTIME_DESCRIPTION = "process.runtime.description";
  private static final String PROCESS_RUNTIME_NAME = "process.runtime.name";
  private static final String PROCESS_RUNTIME_VERSION = "process.runtime.version";
  private static final String SERVICE_NAME = "service.name";
  private static final String SERVICE_NAMESPACE = "service.namespace";
  private static final String SERVICE_VERSION = "service.version";

  private OtelUtil() {}

  public static Attributes addLabels(
      final Attributes attributes, final Map<String, String> labels) {
    return ofNullable(labels)
        .map(
            l ->
                l.entrySet().stream()
                    .reduce(
                        attributes.toBuilder(),
                        (b, e) -> b.put(e.getKey(), e.getValue()),
                        (b1, b2) -> b1))
        .map(AttributesBuilder::build)
        .orElse(attributes);
  }

  public static void addOtelLogHandler(final Logger logger, final OtelLogHandler handler) {
    if (!hasOtelHandler(logger)) {
      logger.addHandler(handler);
    }
  }

  /**
   * Populates the system labels of a resource with Java system properties.
   *
   * @param builder the given builder.
   * @return The updated builder.
   */
  public static ResourceBuilder addSystemProperties(final ResourceBuilder builder) {
    return builder
        .put(OS_NAME, System.getProperty(OS_NAME))
        .put(OS_TYPE, System.getProperty("os.arch"))
        .put(OS_VERSION, System.getProperty(OS_VERSION))
        .put(
            PROCESS_RUNTIME_DESCRIPTION,
            getProperty("java.vm.vendor")
                + " "
                + getProperty("java.vm.name")
                + " "
                + getProperty("java.vm.version"))
        .put(PROCESS_RUNTIME_NAME, getProperty("java.runtime.name"))
        .put(PROCESS_RUNTIME_VERSION, getProperty("java.runtime.version"));
  }

  private static boolean hasOtelHandler(final Logger logger) {
    return ofNullable(logger.getHandlers()).stream()
        .flatMap(Arrays::stream)
        .anyMatch(OtelLogHandler.class::isInstance);
  }

  private static Optional<LogRecordExporter> logRecordExporter(final Config config) {
    return Cases.<Config, LogRecordExporter>withValue(config)
        .orGet(
            c -> configValue(c::getString, GRPC),
            endpoint -> OtlpGrpcLogRecordExporter.builder().setEndpoint(endpoint).build())
        .orGet(
            c -> configValue(c::getString, HTTP),
            endpoint -> OtlpHttpLogRecordExporter.builder().setEndpoint(endpoint).build())
        .get();
  }

  /**
   * Uses the paths <code>otlp.grpc</code> and <code>otlp.http</code> in the configuration to create
   * a processor. The values should be URLs.
   *
   * @param config the given configuration.
   * @return The log record processor.
   */
  public static Optional<LogRecordProcessor> logRecordProcessor(final Config config) {
    return configValue(config::getConfig, OTLP)
        .flatMap(OtelUtil::logRecordExporter)
        .map(e -> BatchLogRecordProcessor.builder(e).build());
  }

  private static Optional<SdkLoggerProvider> loggerProvider(
      final String namespace,
      final String name,
      final String version,
      final LogRecordProcessor processor) {
    return ofNullable(processor)
        .map(
            p ->
                SdkLoggerProvider.builder()
                    .setResource(otelResource(namespace, name, version))
                    .addLogRecordProcessor(p)
                    .build());
  }

  private static Optional<SdkMeterProvider> meterProvider(
      final String namespace, final String name, final String version, final Config config) {
    return configValue(config::getConfig, OTLP)
        .flatMap(OtelUtil::metricExporter)
        .map(PeriodicMetricReader::create)
        .map(
            r ->
                SdkMeterProvider.builder()
                    .addResource(otelResource(namespace, name, version))
                    .registerMetricReader(r)
                    .build());
  }

  private static Optional<MetricExporter> metricExporter(final Config config) {
    return Cases.<Config, MetricExporter>withValue(config)
        .orGet(
            c -> configValue(c::getString, GRPC),
            endpoint -> OtlpGrpcMetricExporter.builder().setEndpoint(endpoint).build())
        .orGet(
            c -> configValue(c::getString, HTTP),
            endpoint -> OtlpHttpMetricExporter.builder().setEndpoint(endpoint).build())
        .get();
  }

  public static Optional<OpenTelemetry> metrics(
      final String namespace, final String name, final String version, final Config config) {
    return meterProvider(namespace, name, version, config)
        .map(p -> OpenTelemetrySdk.builder().setMeterProvider(p).build());
  }

  public static Optional<OtelLogHandler> otelLogHandler(
      final String namespace,
      final String name,
      final String version,
      final LogRecordProcessor processor) {
    return loggerProvider(namespace, name, version, processor)
        .map(p -> OpenTelemetrySdk.builder().setLoggerProvider(p).build())
        .map(OtelLogHandler::new);
  }

  public static Resource otelResource(
      final String namespace, final String name, final String version) {
    return addSystemProperties(
            Resource.getDefault().toBuilder()
                .put(SERVICE_NAMESPACE, namespace)
                .put(SERVICE_NAME, name)
                .put(SERVICE_VERSION, version))
        .build();
  }

  /**
   * Creates observable <code>Long</code> counters that are reset after each fetch.
   *
   * @param meter the meter from which the counters are created.
   * @param name the counter name.
   * @param attributes the function that generates the attributes for each measurement based on the
   *     counted data.
   * @param increment the amount to be added to the counter for an incoming element.
   * @param counters a set where the generated counters are put. At some point the caller should
   *     close all the counters.
   * @return The function that captures the counted data.
   * @param <T> the value type.
   */
  public static <T> Consumer<T> resettingCounter(
      final Meter meter,
      final String name,
      final Function<T, Attributes> attributes,
      final ToLongFunction<T> increment,
      final Set<AutoCloseable> counters) {
    final Map<Attributes, Long> counts = new ConcurrentHashMap<>();
    final Set<Attributes> created = new HashSet<>();

    return message -> {
      final Attributes a = attributes.apply(message);

      counts.put(a, counts.computeIfAbsent(a, k -> 0L) + increment.applyAsLong(message));

      if (!created.contains(a)) {
        created.add(a);
        counters.add(
            meter
                .counterBuilder(name)
                .buildWithCallback(
                    measurement -> {
                      measurement.record(counts.get(a), a);
                      counts.put(a, 0L);
                    }));
      }
    };
  }
}
