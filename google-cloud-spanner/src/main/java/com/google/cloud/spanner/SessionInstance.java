package com.google.cloud.spanner;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.spanner.SessionClient.SessionId;
import com.google.cloud.spanner.spi.v1.SpannerRpc;
import java.util.Map;
import javax.annotation.Nullable;
import org.threeten.bp.Instant;

public class SessionInstance {

  private final String name;
  private final DatabaseId databaseId;
  private final Map<SpannerRpc.Option, ?> options;
  private volatile Instant lastUseTime;
  @Nullable private final Instant createTime;
  private final boolean isMultiplexed;

  SessionInstance(String name, Map<SpannerRpc.Option, ?> options) {
    this.options = options;
    this.name = checkNotNull(name);
    this.databaseId = SessionId.of(name).getDatabaseId();
    this.lastUseTime = Instant.now();
    this.createTime = null;
    this.isMultiplexed = false;
  }

  SessionInstance(
      String name,
      com.google.protobuf.Timestamp createTime,
      boolean isMultiplexed,
      Map<SpannerRpc.Option, ?> options) {
    this.options = options;
    this.name = checkNotNull(name);
    this.databaseId = SessionId.of(name).getDatabaseId();
    this.lastUseTime = Instant.now();
    this.createTime = convert(createTime);
    this.isMultiplexed = isMultiplexed;
  }

  public String getName() {
    return name;
  }

  public DatabaseId getDatabaseId() {
    return databaseId;
  }

  Map<SpannerRpc.Option, ?> getOptions() {
    return options;
  }

  Instant getLastUseTime() {
    return lastUseTime;
  }

  Instant getCreateTime() {
    return createTime;
  }

  boolean getIsMultiplexed() {
    return isMultiplexed;
  }

  void markUsed(Instant instant) {
    lastUseTime = instant;
  }

  private Instant convert(com.google.protobuf.Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    }
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
