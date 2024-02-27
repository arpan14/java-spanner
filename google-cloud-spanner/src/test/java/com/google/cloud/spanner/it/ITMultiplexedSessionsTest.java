
/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.spanner.it;

import static com.google.cloud.spanner.SpannerMatchers.isSpannerException;
import static com.google.cloud.spanner.Type.StructField;
import static com.google.cloud.spanner.testing.EmulatorSpannerHelper.isUsingEmulator;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.IntegrationTestEnv;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ParallelIntegrationTest;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.spanner.v1.DirectedReadOptions;
import com.google.spanner.v1.DirectedReadOptions.IncludeReplicas;
import com.google.spanner.v1.DirectedReadOptions.ReplicaSelection;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests for multiplexed sessions.
 *
 * <p>See also {@link ITWriteTest}, which provides coverage of writing and reading back all Cloud
 * Spanner types.
 */
@Category(ParallelIntegrationTest.class)
@RunWith(Parameterized.class)
public class ITMultiplexedSessionsTest {
  @ClassRule public static IntegrationTestEnv env = new IntegrationTestEnv();
  private static final String TABLE_NAME = "TestTable";
  private static final List<String> ALL_COLUMNS = Arrays.asList("Key", "StringValue");
  private static DatabaseClient googleStandardSQLClient;
  private static DatabaseClient postgreSQLClient;

  @BeforeClass
  public static void setUpDatabase() {
    Database googleStandardSQLDatabase =
        env.getTestHelper()
            .createTestDatabase(
                "CREATE TABLE TestTable ("
                    + "  key                STRING(MAX) NOT NULL,"
                    + "  stringvalue        STRING(MAX),"
                    + ") PRIMARY KEY (key)",
                "CREATE INDEX TestTableByValue ON TestTable(stringvalue)",
                "CREATE INDEX TestTableByValueDesc ON TestTable(stringvalue DESC)");
    googleStandardSQLClient = env.getTestHelper().getDatabaseClient(googleStandardSQLDatabase);
    if (!isUsingEmulator()) {
      Database postgreSQLDatabase =
          env.getTestHelper()
              .createTestDatabase(
                  Dialect.POSTGRESQL,
                  Arrays.asList(
                      "CREATE TABLE TestTable ("
                          + "  Key                VARCHAR PRIMARY KEY,"
                          + "  StringValue        VARCHAR"
                          + ")",
                      "CREATE INDEX TestTableByValue ON TestTable(StringValue)",
                      "CREATE INDEX TestTableByValueDesc ON TestTable(StringValue DESC)"));
      postgreSQLClient = env.getTestHelper().getDatabaseClient(postgreSQLDatabase);
    }

    // Includes k0..k14.  Note that strings k{10,14} sort between k1 and k2.
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 15; ++i) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder(TABLE_NAME)
              .set("key")
              .to("k" + i)
              .set("stringvalue")
              .to("v" + i)
              .build());
    }
    googleStandardSQLClient.write(mutations);
    if (!isUsingEmulator()) {
      postgreSQLClient.write(mutations);
    }
  }

  @AfterClass
  public static void teardown() {
    ConnectionOptions.closeSpanner();
  }

  @Parameterized.Parameters(name = "Dialect = {0}")
  public static List<DialectTestParameter> data() {
    List<DialectTestParameter> params = new ArrayList<>();
    params.add(new DialectTestParameter(Dialect.GOOGLE_STANDARD_SQL));
    // "PG dialect tests are not supported by the emulator"
    if (!isUsingEmulator()) {
      params.add(new DialectTestParameter(Dialect.POSTGRESQL));
    }
    return params;
  }

  @Parameterized.Parameter(0)
  public DialectTestParameter dialect;

  private DatabaseClient getClient(Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return postgreSQLClient;
    }
    return googleStandardSQLClient;
  }

  @Test
  public void pointRead() {
    Struct row =
        getClient(dialect.dialect)
            .singleUse(TimestampBound.strong())
            .readRow(TABLE_NAME, Key.of("k1"), ALL_COLUMNS);
    assertThat(row).isNotNull();
    assertThat(row.getString(0)).isEqualTo("k1");
    assertThat(row.getString(1)).isEqualTo("v1");
    // Ensure that the Struct implementation supports equality properly.
    assertThat(row)
        .isEqualTo(Struct.newBuilder().set("key").to("k1").set("stringvalue").to("v1").build());
  }
}
