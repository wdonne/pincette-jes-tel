package net.pincette.jes.tel;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import io.opentelemetry.api.common.Attributes;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.pincette.jes.tel.OtelLogHandler.OtelLogRecord;

public class OtelLogger {
  private OtelLogger() {}

  public static void fine(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, FINE, null, message, attributes);
  }

  public static void fine(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, FINE, null, message, attributes, traceId, spanId);
  }

  public static void finer(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, FINER, null, message, attributes);
  }

  public static void finer(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, FINER, null, message, attributes, traceId, spanId);
  }

  public static void finest(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, FINEST, null, message, attributes);
  }

  public static void finest(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, FINEST, null, message, attributes, traceId, spanId);
  }

  public static void info(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, INFO, null, message, attributes);
  }

  public static void info(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, INFO, null, message, attributes, traceId, spanId);
  }

  public static void log(
      final Logger logger,
      final Level level,
      final Supplier<String> message,
      final Supplier<Attributes> attributes) {
    log(logger, level, null, message, attributes);
  }

  public static void log(
      final Logger logger,
      final Level level,
      final Throwable thrown,
      final Supplier<String> message,
      final Supplier<Attributes> attributes) {
    log(logger, level, thrown, message, attributes, null, null);
  }

  public static void log(
      final Logger logger,
      final Level level,
      final Throwable thrown,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    if (logger.isLoggable(level)) {
      final OtelLogRecord rec = new OtelLogRecord(level, attributes.get(), traceId, spanId);

      if (thrown != null) {
        rec.setThrown(thrown);
      }

      rec.setLoggerName(logger.getName());
      rec.setMessage(message.get());
      logger.log(rec);
    }
  }

  public static void severe(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, SEVERE, null, message, attributes);
  }

  public static void severe(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, SEVERE, null, message, attributes, traceId, spanId);
  }

  public static void warning(
      final Logger logger, final Supplier<String> message, final Supplier<Attributes> attributes) {
    log(logger, WARNING, null, message, attributes);
  }

  public static void warning(
      final Logger logger,
      final Supplier<String> message,
      final Supplier<Attributes> attributes,
      final String traceId,
      final String spanId) {
    log(logger, WARNING, null, message, attributes, traceId, spanId);
  }
}
