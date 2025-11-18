package org.flexlb.telemetry.extension;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.trace.IdGenerator;

/** 自定义的 trace id 生成器 */
public class CustomIdGenerator implements IdGenerator {

  public static final String[] APPEND_ZEROS = makeConst();

  public static final IdGenerator randomGenerator = IdGenerator.random();

  private static String[] makeConst() {
    String[] tmp = new String[33];

    StringBuilder buf = new StringBuilder(tmp.length);
    for (int i = 0; i < tmp.length; i++) {
      tmp[i] = buf.toString();
      buf.append("0");
    }
    return tmp;
  }

  @Override
  public String generateSpanId() {
    return randomGenerator.generateSpanId();
  }

  @Override
  public String generateTraceId() {
    String traceId = Span.current().getSpanContext().getTraceId();
    if (traceId.length() == TraceId.getLength()) {
      return traceId;
    }

    return traceId + APPEND_ZEROS[TraceId.getLength() - traceId.length()];
  }
}
