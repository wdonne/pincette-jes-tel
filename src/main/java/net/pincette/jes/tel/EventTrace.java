package net.pincette.jes.tel;

import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.util.ImmutableBuilder.create;

import java.time.Instant;
import java.util.Map;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import net.pincette.json.JsonUtil;

/**
 * This represents the occurrence of an event. It can be used to build distributed event traces.
 *
 * @author Werner Donné
 */
public class EventTrace {
  private static final String ATTRIBUTES_FIELD = "attributes";
  private static final String MODULE_NAME_FIELD = "moduleName";
  private static final String MODULE_VERSION_FIELD = "moduleVersion";
  private static final String NAME_FIELD = "name";
  private static final String SERVICE_NAME_FIELD = "serviceName";
  private static final String SERVICE_NAMESPACE_FIELD = "serviceNamespace";
  private static final String SERVICE_VERSION_FIELD = "serviceVersion";
  private static final String PAYLOAD_FIELD = "payload";
  private static final String TIMESTAMP_NANOS_FIELD = "timestampNanos";
  private static final String TRACE_ID_FIELD = "traceId";
  private static final String USERNAME_FIELD = "username";

  public final Map<String, ?> attributes;
  public final String moduleName;
  public final String moduleVersion;
  public final String name;
  public final JsonObject payload;
  public final String serviceName;
  public final String serviceNamespace;
  public final String serviceVersion;
  public final Instant timestamp;
  public final String traceId;
  public final String username;

  public EventTrace() {
    this(null, null, null, null, null, null, null, null, now(), null, null);
  }

  @SuppressWarnings("java:S107") // Internal constructor to provide immutability.
  private EventTrace(
      final Map<String, ?> attributes,
      final String moduleName,
      final String moduleVersion,
      final String name,
      final JsonObject payload,
      final String serviceName,
      final String serviceNamespace,
      final String serviceVersion,
      final Instant timestamp,
      final String traceId,
      final String username) {
    this.attributes = attributes;
    this.moduleName = moduleName;
    this.moduleVersion = moduleVersion;
    this.name = name;
    this.payload = payload;
    this.serviceName = serviceName;
    this.serviceNamespace = serviceNamespace;
    this.serviceVersion = serviceVersion;
    this.timestamp = timestamp;
    this.traceId = traceId != null ? traceId.toLowerCase() : null;
    this.username = username;
  }

  public JsonObjectBuilder toJson() {
    return create(JsonUtil::createObjectBuilder)
        .updateIf(() -> ofNullable(attributes), (b, a) -> b.add(ATTRIBUTES_FIELD, from(attributes)))
        .updateIf(() -> ofNullable(moduleName), (b, n) -> b.add(MODULE_NAME_FIELD, n))
        .updateIf(() -> ofNullable(moduleVersion), (b, n) -> b.add(MODULE_VERSION_FIELD, n))
        .updateIf(() -> ofNullable(name), (b, n) -> b.add(NAME_FIELD, n))
        .updateIf(() -> ofNullable(payload), (b, p) -> b.add(PAYLOAD_FIELD, p))
        .updateIf(() -> ofNullable(serviceName), (b, n) -> b.add(SERVICE_NAME_FIELD, n))
        .updateIf(() -> ofNullable(serviceNamespace), (b, n) -> b.add(SERVICE_NAMESPACE_FIELD, n))
        .updateIf(() -> ofNullable(serviceVersion), (b, n) -> b.add(SERVICE_VERSION_FIELD, n))
        .updateIf(
            () -> ofNullable(timestamp),
            (b, t) -> b.add(TIMESTAMP_NANOS_FIELD, t.getEpochSecond() * 1000000000 + t.getNano()))
        .updateIf(() -> ofNullable(traceId), (b, t) -> b.add(TRACE_ID_FIELD, t))
        .updateIf(() -> ofNullable(username), (b, u) -> b.add(USERNAME_FIELD, u))
        .build();
  }

  public EventTrace withAttributes(final Map<String, String> attributes) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withModuleName(final String moduleName) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withModuleVersion(final String moduleVersion) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withName(final String name) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withPayload(final JsonObject payload) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withServiceName(final String serviceName) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withServiceNamespace(final String serviceNamespace) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withServiceVersion(final String serviceVersion) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withTimestamp(final Instant timestamp) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withTraceId(final String traceId) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }

  public EventTrace withUsername(final String username) {
    return new EventTrace(
        attributes,
        moduleName,
        moduleVersion,
        name,
        payload,
        serviceName,
        serviceNamespace,
        serviceVersion,
        timestamp,
        traceId,
        username);
  }
}
