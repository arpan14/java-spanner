/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner;

/** Class to represent auto-generated RPC options. The options in this class
 * are auto-generated using GAPIC auto-gen
 */
class AutoGeneratedOptions {

  /** Specifies the maximum batching delay for the transaction. */
  public static Options.TransactionOption maxBatchingDelayMs(int maxBatchingDelayMs) {
    return new MaxBatchingDelayMsOption(maxBatchingDelayMs);
  }

  /**
   * TODO - How does auto-gen understand that this option needs to implement {@link
   * Options.TransactionOption}
   */
  static final class MaxBatchingDelayMsOption extends InternalOption
      implements Options.TransactionOption {
    final int maxBatchingDelayMs;

    MaxBatchingDelayMsOption(int maxBatchingDelayMs) {
      this.maxBatchingDelayMs = maxBatchingDelayMs;
    }

    @Override
    void appendToOptions(Options options) {
      options.maxBatchingDelayMs = maxBatchingDelayMs;
    }
  }

  Integer maxBatchingDelayMs;

  boolean hasMaxBatchingDelayMs() {
    return maxBatchingDelayMs != null;
  }

  int maxBatchingDelayMs() {
    return maxBatchingDelayMs;
  }

  // TODO - How does auto-gen modify the equals and hashcode methods of {@link Options}
}
