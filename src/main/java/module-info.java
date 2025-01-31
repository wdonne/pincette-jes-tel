module net.pincette.jes.tel {
  requires io.opentelemetry.api;
  requires io.opentelemetry.sdk.common;
  requires io.opentelemetry.exporter.otlp;
  requires io.opentelemetry.sdk.logs;
  requires io.opentelemetry.sdk;
  requires net.pincette.config;
  requires typesafe.config;
  requires io.opentelemetry.sdk.metrics;
  requires net.pincette.netty.http;
  requires net.pincette.rs;
  requires net.pincette.common;
  requires net.pincette.json;
  requires java.logging;
  requires java.json;
  requires io.opentelemetry.context;

  exports net.pincette.jes.tel;
}
