/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/spanner/executor/v1/cloud_executor.proto

package com.google.spanner.executor.v1;

public interface PartitionedUpdateActionOrBuilder
    extends
    // @@protoc_insertion_point(interface_extends:google.spanner.executor.v1.PartitionedUpdateAction)
    com.google.protobuf.MessageOrBuilder {

  /**
   *
   *
   * <pre>
   * Options for partitioned update.
   * </pre>
   *
   * <code>
   * optional .google.spanner.executor.v1.PartitionedUpdateAction.ExecutePartitionedUpdateOptions options = 1;
   * </code>
   *
   * @return Whether the options field is set.
   */
  boolean hasOptions();
  /**
   *
   *
   * <pre>
   * Options for partitioned update.
   * </pre>
   *
   * <code>
   * optional .google.spanner.executor.v1.PartitionedUpdateAction.ExecutePartitionedUpdateOptions options = 1;
   * </code>
   *
   * @return The options.
   */
  com.google.spanner.executor.v1.PartitionedUpdateAction.ExecutePartitionedUpdateOptions
      getOptions();
  /**
   *
   *
   * <pre>
   * Options for partitioned update.
   * </pre>
   *
   * <code>
   * optional .google.spanner.executor.v1.PartitionedUpdateAction.ExecutePartitionedUpdateOptions options = 1;
   * </code>
   */
  com.google.spanner.executor.v1.PartitionedUpdateAction.ExecutePartitionedUpdateOptionsOrBuilder
      getOptionsOrBuilder();

  /**
   *
   *
   * <pre>
   * Partitioned dml query.
   * </pre>
   *
   * <code>.google.spanner.executor.v1.QueryAction update = 2;</code>
   *
   * @return Whether the update field is set.
   */
  boolean hasUpdate();
  /**
   *
   *
   * <pre>
   * Partitioned dml query.
   * </pre>
   *
   * <code>.google.spanner.executor.v1.QueryAction update = 2;</code>
   *
   * @return The update.
   */
  com.google.spanner.executor.v1.QueryAction getUpdate();
  /**
   *
   *
   * <pre>
   * Partitioned dml query.
   * </pre>
   *
   * <code>.google.spanner.executor.v1.QueryAction update = 2;</code>
   */
  com.google.spanner.executor.v1.QueryActionOrBuilder getUpdateOrBuilder();
}
