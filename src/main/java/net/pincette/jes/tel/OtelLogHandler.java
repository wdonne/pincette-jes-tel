package net.pincette.jes.tel;

import static io.opentelemetry.api.logs.Severity.DEBUG;
import static io.opentelemetry.api.logs.Severity.DEBUG2;
import static io.opentelemetry.api.logs.Severity.DEBUG3;
import static io.opentelemetry.api.logs.Severity.ERROR;
import static io.opentelemetry.api.logs.Severity.UNDEFINED_SEVERITY_NUMBER;
import static io.opentelemetry.api.logs.Severity.WARN;
import static io.opentelemetry.api.trace.Span.wrap;
import static io.opentelemetry.context.Context.current;
import static java.text.MessageFormat.format;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.CONFIG;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.OFF;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Util.getStackTrace;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.LoggerProvider;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import net.pincette.util.Cases;

/**
 * Emits log records to the given <code>OpenTelemetry</code> instance.
 *
 * @author Werner Donné
 */
public class OtelLogHandler extends Handler {
  private static final String CODE_FUNCTION = "code.function";
  private static final String CODE_NAMESPACE = "code.namespace";
  private static final String EXCEPTION_MESSAGE = "exception.message";
  private static final String EXCEPTION_STACKTRACE = "exception.stacktrace";
  private static final String EXCEPTION_TYPE = "exception.type";
  private static final String LOG_RECORD_UID = "log.record.uid";
  private static final String SEQ = "seq";
  private static final String THREAD_ID = "thread.id";

  private final LoggerProvider loggerProvider;

  public OtelLogHandler(final OpenTelemetry openTelemetry) {
    this.loggerProvider = openTelemetry.getLogsBridge();
  }

  private static Context addIds(final Context context, final OtelLogRecord rec) {
    return context.with(
        wrap(
            SpanContext.create(
                rec.traceId, rec.spanId, TraceFlags.getDefault(), TraceState.getDefault())));
  }

  private static Attributes attributes(final LogRecord rec) {
    return create(Attributes::builder)
        .updateIf(() -> ofNullable(rec.getSourceClassName()), (b, c) -> b.put(CODE_NAMESPACE, c))
        .updateIf(() -> ofNullable(rec.getSourceMethodName()), (b, m) -> b.put(CODE_FUNCTION, m))
        .updateIf(
            () -> ofNullable(rec.getThrown()),
            (b, t) ->
                b.put(EXCEPTION_MESSAGE, t.getMessage())
                    .put(EXCEPTION_TYPE, t.getClass().getName())
                    .put(EXCEPTION_STACKTRACE, getStackTrace(t)))
        .updateIf(
            () ->
                Optional.of(rec)
                    .filter(OtelLogRecord.class::isInstance)
                    .map(r -> (OtelLogRecord) r)
                    .map(r -> r.attributes),
            AttributesBuilder::putAll)
        .update(b -> b.put(LOG_RECORD_UID, randomUUID().toString()))
        .update(b -> b.put(SEQ, rec.getSequenceNumber()))
        .update(b -> b.put(THREAD_ID, rec.getLongThreadID()))
        .build()
        .build();
  }

  private static Context context(final LogRecord rec) {
    return rec instanceof OtelLogRecord r ? addIds(current(), r) : current();
  }

  private static String message(final LogRecord rec) {
    return Cases.<LogRecord, String>withValue(rec)
        .orGet(r -> ofNullable(rec.getResourceBundle()), b -> b.getString(rec.getMessage()))
        .orGet(r -> ofNullable(r.getParameters()), p -> format(rec.getMessage(), p))
        .get()
        .orElseGet(rec::getMessage);
  }

  private static Severity severity(final Level level) {
    return Cases.<Level, Severity>withValue(level)
        .or(SEVERE::equals, l -> ERROR)
        .or(WARNING::equals, l -> WARN)
        .or(l -> INFO.equals(l) || CONFIG.equals(l), l -> Severity.INFO)
        .or(FINE::equals, l -> DEBUG)
        .or(FINER::equals, l -> DEBUG2)
        .or(FINEST::equals, l -> DEBUG3)
        .get()
        .orElse(UNDEFINED_SEVERITY_NUMBER);
  }

  @Override
  public void close() throws SecurityException {
    // Nothing to do.
  }

  @Override
  public void flush() {
    // Nothing to do.
  }

  @Override
  public void publish(final LogRecord rec) {
    if (!rec.getLevel().equals(OFF)) {
      loggerProvider
          .get(rec.getLoggerName())
          .logRecordBuilder()
          .setObservedTimestamp(rec.getInstant())
          .setTimestamp(rec.getInstant())
          .setBody(ofNullable(message(rec)).orElse(""))
          .setSeverity(severity(rec.getLevel()))
          .setContext(context(rec))
          .setAllAttributes(attributes(rec))
          .emit();
    }
  }

  static class OtelLogRecord extends LogRecord {
    private final transient Attributes attributes;
    private final transient String spanId;
    private final transient String traceId;

    OtelLogRecord(final Level level, final Attributes attributes) {
      this(level, attributes, null, null);
    }

    OtelLogRecord(
        final Level level, final Attributes attributes, final String traceId, final String spanId) {
      super(level, null);
      this.attributes = attributes;
      this.spanId = spanId;
      this.traceId = traceId;
    }
  }
}
