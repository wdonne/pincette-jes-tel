package net.pincette.jes.tel;

import static java.lang.Math.max;
import static java.util.Optional.ofNullable;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.util.Util.with;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import net.pincette.netty.http.Metrics;

/**
 * Utilities for metrics to work with <code>net.pincette.netty.http</code>.
 *
 * @author Werner Donné
 */
public class HttpMetrics {
  private static final String HTTP_REQUEST_METHOD = "http.request.method";
  private static final String HTTP_RESPONSE_STATUS_CODE = "http.response.status_code";
  private static final String HTTP_SERVER_REQUESTS = "http.server.requests";
  private static final String HTTP_SERVER_AVERAGE_DURATION_MILLIS =
      "http.server.average_duration_millis";
  private static final String HTTP_SERVER_AVERAGE_REQUEST_BYTES =
      "http.server.average_request_bytes";
  private static final String HTTP_SERVER_AVERAGE_RESPONSE_BYTES =
      "http.server.average_response_bytes";
  private static final String INSTANCE = "instance";
  private static final String NETWORK_PROTOCOL_NAME = "network.protocol.name";
  private static final String URL_SCHEME = "url.scheme";

  private HttpMetrics() {}

  private static Attributes addDimensions(
      final Attributes attributes, final Metrics metrics, final String instance) {
    return ofNullable(attributes).orElseGet(Attributes::empty).toBuilder()
        .put(HTTP_REQUEST_METHOD, metrics.method())
        .put(HTTP_RESPONSE_STATUS_CODE, metrics.statusCode())
        .put(INSTANCE, instance)
        .build();
  }

  /**
   * Sets standard attributes that are relevant for aggregated metrics.
   *
   * @param metrics the received data point.
   * @return The attributes.
   */
  public static Attributes attributes(final Metrics metrics) {
    return Attributes.builder()
        .put(NETWORK_PROTOCOL_NAME, metrics.protocol())
        .put(URL_SCHEME, metrics.protocol())
        .build();
  }

  private static void recordAverage(
      final ObservableLongMeasurement measurement, final Map<Attributes, AverageCounter> counters) {
    counters.forEach((k, v) -> measurement.record(v.consumeAverage(), k));
  }

  private static void recordValue(
      final ObservableLongMeasurement measurement, final Map<Attributes, Long> counters) {
    counters.forEach(
        (k, v) -> {
          measurement.record(v, k);
          counters.put(k, 0L);
        });
  }

  /**
   * Consumes HTTP metrics with which the counters <code>http.server.average_duration_millis</code>,
   * <code>http.server.average_request_bytes</code>, <code>http.server.average_response_bytes</code>
   * and <code>http.server.request</code> are fed. The former three keep the averages within the
   * measurement interval, which is defined in the meter. All counters are reset after each
   * measurement interval.
   *
   * @param meter the meter object from which the counters are created.
   * @param pathDimensions the function that creates additional attributes for the metrics. The
   *     higher the number of dimensions, the more internal counters there will be. The function may
   *     return <code>null</code>.
   * @param instance represents a running instance in order to cope with the case where multiple
   *     instances are present.
   * @return The subscriber.
   */
  public static Subscriber<Metrics> subscriber(
      final Meter meter, final Function<String, Attributes> pathDimensions, final String instance) {
    final Map<Attributes, AverageCounter> durationMillis = new ConcurrentHashMap<>();
    final AutoCloseable durationMillisCounter =
        meter
            .counterBuilder(HTTP_SERVER_AVERAGE_DURATION_MILLIS)
            .buildWithCallback(measurement -> recordAverage(measurement, durationMillis));
    final Map<Attributes, AverageCounter> requestBytes = new ConcurrentHashMap<>();
    final AutoCloseable requestBytesCounter =
        meter
            .counterBuilder(HTTP_SERVER_AVERAGE_REQUEST_BYTES)
            .buildWithCallback(measurement -> recordAverage(measurement, requestBytes));
    final Map<Attributes, AverageCounter> responseBytes = new ConcurrentHashMap<>();
    final AutoCloseable responseBytesCounter =
        meter
            .counterBuilder(HTTP_SERVER_AVERAGE_RESPONSE_BYTES)
            .buildWithCallback(measurement -> recordAverage(measurement, responseBytes));
    final Map<Attributes, Long> requests = new ConcurrentHashMap<>();
    final AutoCloseable requestsCounter =
        meter
            .counterBuilder(HTTP_SERVER_REQUESTS)
            .buildWithCallback(measurement -> recordValue(measurement, requests));

    return lambdaSubscriber(
        metrics -> {
          final Attributes attributes =
              addDimensions(pathDimensions.apply(metrics.path()), metrics, instance);

          with(
              () -> durationMillis.computeIfAbsent(attributes, k -> new AverageCounter()),
              c -> c.add(metrics.timeTaken().toMillis()));
          with(
              () -> requestBytes.computeIfAbsent(attributes, k -> new AverageCounter()),
              c -> c.add(metrics.requestBytes()));
          with(
              () -> responseBytes.computeIfAbsent(attributes, k -> new AverageCounter()),
              c -> c.add(metrics.responseBytes()));
          requests.put(attributes, requests.computeIfAbsent(attributes, k -> 0L) + 1);
        },
        () -> {
          durationMillisCounter.close();
          requestBytesCounter.close();
          responseBytesCounter.close();
          requestsCounter.close();
        });
  }

  private static class AverageCounter {
    private long requests;
    private long value;

    private AverageCounter add(final long value) {
      this.value += value;
      requests += 1;

      return this;
    }

    private long consumeAverage() {
      final long result = value / max(requests, 1);

      value = 0L;
      requests = 0L;

      return result;
    }
  }
}
