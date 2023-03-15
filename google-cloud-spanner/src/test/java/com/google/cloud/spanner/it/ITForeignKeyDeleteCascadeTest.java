package com.google.cloud.spanner.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.IntegrationTestEnv;
import com.google.cloud.spanner.ParallelIntegrationTest;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
@Category(ParallelIntegrationTest.class)
@RunWith(JUnit4.class)
public class ITForeignKeyDeleteCascadeTest {

  private static final String CREATE_TABLE_SINGER = "CREATE TABLE Singer (\n"
      + "  SingerId   INT64 NOT NULL,\n"
      + "  FirstName  STRING(1024),\n"
      + ") PRIMARY KEY(SingerId)\n";

  final String CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "  VenueId      INT64 NOT NULL,\n"
      + "  SingerId     INT64 NOT NULL,\n"
      + "  CONSTRAINT Fk_Concerts_Singer FOREIGN KEY (SingerId) REFERENCES Singer (SingerId) ON DELETE CASCADE"
      + ") PRIMARY KEY(VenueId, SingerId)";

  final String CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "  VenueId      INT64 NOT NULL,\n"
      + "  SingerId     INT64 NOT NULL,\n"
      + ") PRIMARY KEY(VenueId, SingerId)";

  final String ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY = "ALTER TABLE Concerts "
      + "ADD CONSTRAINT Fk_Concerts_Singer FOREIGN KEY(SingerId) REFERENCES Singer(SingerId) "
      + "ON DELETE CASCADE";
  private static final String CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME = "Fk_Concerts_Singer";

  @ClassRule
  public static IntegrationTestEnv env = new IntegrationTestEnv();
  private DatabaseAdminClient dbAdminClient;
  private RemoteSpannerHelper testHelper;
  private List<Database> dbs = new ArrayList<>();

  @Before
  public void setUp() {
    testHelper = env.getTestHelper();
    dbAdminClient = testHelper.getClient().getDatabaseAdminClient();
  }

  @After
  public void tearDown() {
    for (Database db : dbs) {
      db.drop();
    }
    dbs.clear();
  }

  @Test
  public void testForeignKeyDeleteCascadeConstraints_withCreateDDLStatements() throws Exception {
    final String databaseId = testHelper.getUniqueDatabaseId();
    final String instanceId = testHelper.getInstanceId().getInstance();

    final Database createdDatabase =
        dbAdminClient
            .createDatabase(instanceId, databaseId,
                ImmutableList.of(CREATE_TABLE_SINGER, CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY))
            .get(5, TimeUnit.MINUTES);
    dbs.add(createdDatabase);
    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);

    try (final ResultSet rs =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT DELETE_RULE,CONSTRAINT_NAME\n"
                        + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS\n"
                        + "WHERE CONSTRAINT_NAME =" + "\"" + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME
                        + "\""))) {

      final List<String> foreignKeyConstraints = new ArrayList<>();
      while (rs.next()) {
        foreignKeyConstraints.add(
            rs.getString("CONSTRAINT_NAME"));
      }

      assertThat(foreignKeyConstraints).isEqualTo(
          ImmutableList.of(CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME));
    }
  }

  @Test
  public void testForeignKeyDeleteCascadeConstraints_withAlterDDLStatements() throws Exception {
    final String databaseId = testHelper.getUniqueDatabaseId();
    final String instanceId = testHelper.getInstanceId().getInstance();

    final Database createdDatabase =
        dbAdminClient
            .createDatabase(instanceId, databaseId,
                ImmutableList.of(CREATE_TABLE_SINGER, CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY,
                    ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY))
            .get(5, TimeUnit.MINUTES);
    dbs.add(createdDatabase);
    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);

    try (final ResultSet rs =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT DELETE_RULE,CONSTRAINT_NAME\n"
                        + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS\n"
                        + "WHERE CONSTRAINT_NAME =" + "\"" + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME
                        + "\""))) {

      final List<String> foreignKeyConstraints = new ArrayList<>();
      while (rs.next()) {
        foreignKeyConstraints.add(
            rs.getString("CONSTRAINT_NAME"));
      }

      assertThat(foreignKeyConstraints).isEqualTo(
          ImmutableList.of(CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME));
    }
  }
}
