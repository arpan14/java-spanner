package com.google.cloud.spanner;

import com.google.spanner.v1.RequestOptions;


// TODO why does getTransactionTag() return null at some places. For ex {@link AbstractReadContext}
// TODO why does requestTag not always get set. Should we always be setting it over here?
class RequestOptionsBuilder {

  static RequestOptions.Builder build(Options options) {
    RequestOptions.Builder builder = RequestOptions.newBuilder();
    if (options.hasPriority()) {
      builder.setPriority(options.priority());
    }
    if (options.hasTag()) {
      builder.setRequestTag(options.tag());
    }
    return builder;
  }
}
