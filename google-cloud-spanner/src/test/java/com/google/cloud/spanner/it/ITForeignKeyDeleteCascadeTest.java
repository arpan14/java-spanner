package com.google.cloud.spanner.it;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.IntegrationTestEnv;
import com.google.cloud.spanner.ParallelIntegrationTest;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(ParallelIntegrationTest.class)
@RunWith(Parameterized.class)
public class ITForeignKeyDeleteCascadeTest {
  private static final String TABLE_NAME_SINGER = "Singer";
  private static final String TABLE_NAME_CONCERTS = "Concerts";
  private static final String CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME = "Fk_Concerts_Singer";
  private static final String DELETE_RULE_CASCADE = "CASCADE";
  private static final String DELETE_RULE_DEFAULT = "NO ACTION";
  private static final String DELETE_RULE_COLUMN_NAME = "DELETE_RULE";
  private static final String CREATE_TABLE_SINGER = "CREATE TABLE Singer (\n"
      + "  SingerId   INT64 NOT NULL,\n"
      + "  FirstName  STRING(1024),\n"
      + ") PRIMARY KEY(SingerId)\n";

  private static final String POSTGRES_CREATE_TABLE_SINGER = "CREATE TABLE Singer (\n"
      + "  singer_id   BIGINT PRIMARY KEY,\n"
      + "  first_name  VARCHAR\n"
      + ")";

  private static final String CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "  VenueId      INT64 NOT NULL,\n"
      + "  SingerId     INT64 NOT NULL,\n"
      + "  CONSTRAINT Fk_Concerts_Singer FOREIGN KEY (SingerId) REFERENCES Singer (SingerId) ON DELETE CASCADE"
      + ") PRIMARY KEY(VenueId, SingerId)";

  private static final String POSTGRES_CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "      venue_id      BIGINT NOT NULL,\n"
      + "      singer_id     BIGINT NOT NULL,\n"
      + "      PRIMARY KEY (venue_id, singer_id),\n"
      + "      CONSTRAINT Fk_Concerts_Singer FOREIGN KEY (singer_id) REFERENCES Singer (singer_id) ON DELETE CASCADE\n"
      + "      )";

  private static final String CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "  VenueId      INT64 NOT NULL,\n"
      + "  SingerId     INT64 NOT NULL,\n"
      + ") PRIMARY KEY(VenueId, SingerId)";

  private static final String POSTGRES_CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY = "CREATE TABLE Concerts (\n"
      + "      venue_id      BIGINT NOT NULL,\n"
      + "      singer_id     BIGINT NOT NULL,\n"
      + "      PRIMARY KEY (venue_id, singer_id)\n"
      + "      )";

  private static final String ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY = "ALTER TABLE Concerts "
      + "ADD CONSTRAINT " + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME + " FOREIGN KEY(SingerId) REFERENCES Singer(SingerId) "
      + "ON DELETE CASCADE";

  private static final String POSTGRES_ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY = "ALTER TABLE Concerts "
      + "ADD CONSTRAINT " + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME + " FOREIGN KEY(singer_id) REFERENCES Singer(singer_id) "
      + "ON DELETE CASCADE";

  private static final String ALTER_TABLE_CONCERT_UPDATE_FOREIGN_KEY_WITHOUT_DELETE_CASCADE = "ALTER TABLE Concerts "
      + "ADD CONSTRAINT " + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME + " FOREIGN KEY(SingerId) REFERENCES Singer(SingerId) ";

  private static final String POSTGRES_ALTER_TABLE_CONCERT_UPDATE_FOREIGN_KEY_WITHOUT_DELETE_CASCADE = "ALTER TABLE Concerts "
      + "ADD CONSTRAINT " + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME
      + " FOREIGN KEY(singer_id) REFERENCES Singer(singer_id) ";

  private static final String ALTER_TABLE_CONCERT_DROP_FOREIGN_KEY_CONSTRAINT = "ALTER TABLE Concerts\n"
      + "DROP CONSTRAINT " + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME;

  private static final String QUERY_REFERENTIAL_CONSTRAINTS = "SELECT DELETE_RULE\n"
      + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS\n"
      + "WHERE CONSTRAINT_NAME =" + "\"" + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME
      + "\"";
  private static final String POSTGRES_QUERY_REFERENTIAL_CONSTRAINTS = "SELECT DELETE_RULE\n"
      + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS\n"
      + "WHERE CONSTRAINT_NAME =" + "'" + CONCERTS_SINGER_FOREIGN_KEY_CONSTRAINT_NAME
      + "'";

  @ClassRule
  public static IntegrationTestEnv env = new IntegrationTestEnv();
  private DatabaseAdminClient dbAdminClient;
  private RemoteSpannerHelper testHelper;
  private List<Database> dbs = new ArrayList<>();

  @Parameterized.Parameters(name = "Dialect = {0}")
  public static List<DialectTestParameter> data() {
    List<DialectTestParameter> params = new ArrayList<>();
    params.add(new DialectTestParameter(Dialect.GOOGLE_STANDARD_SQL));
    params.add(new DialectTestParameter(Dialect.POSTGRESQL));
    return params;
  }

  @Parameterized.Parameter(0)
  public DialectTestParameter dialect;

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
  public void testForeignKeyDeleteCascadeConstraints_withCreateDDLStatements() {
    final List<String> createStatements = getCreateTableStatementsWithForeignKey();
    final Database createdDatabase = createDatabase(createStatements);
    dbs.add(createdDatabase);
    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);
    final String referentialConstraintQuery = getReferentialConstraintsQueryStatement();
    try (final ResultSet rs =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of(referentialConstraintQuery))) {
      while (rs.next()) {
        assertThat(rs.getString(DELETE_RULE_COLUMN_NAME)).isEqualTo(DELETE_RULE_CASCADE);
      }
    }
  }
  @Test
  public void testForeignKeyDeleteCascadeConstraints_withAlterDDLStatements() throws Exception {
    final List<String> createStatements = getCreateAndAlterTableStatementsWithForeignKey();
    final Database createdDatabase = createDatabase(createStatements);
    dbs.add(createdDatabase);

    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);

    final String referentialConstraintQuery = getReferentialConstraintsQueryStatement();
    try (final ResultSet rs =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.of(referentialConstraintQuery))) {
      while (rs.next()) {
        assertThat(rs.getString(DELETE_RULE_COLUMN_NAME)).isEqualTo(DELETE_RULE_CASCADE);
      }
    }

    // remove the foreign key delete cascade constraint
    final List<String> alterDropStatements = getAlterDropForeignKeyDeleteCascadeStatements();
    dbAdminClient
        .updateDatabaseDdl(env.getTestHelper().getInstanceId().getInstance(),
            createdDatabase.getId().getDatabase(), alterDropStatements, null)
        .get();

    try (final ResultSet rs =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of(referentialConstraintQuery))) {
      while (rs.next()) {
        assertThat(rs.getString(DELETE_RULE_COLUMN_NAME)).isEqualTo(DELETE_RULE_DEFAULT);
      }
    }
  }

  @Test
  public void testForeignKeyDeleteCascadeConstraints_verifyValidInsertions() {
    final List<String> createStatements = getCreateTableStatementsWithForeignKey();
    final Database createdDatabase = createDatabase(createStatements);
    dbs.add(createdDatabase);

    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);
    final String singerInsertStatement = getInsertStatementForSingerTable();
    final Statement singerInsertStatementWithValues = Statement.newBuilder(singerInsertStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(1L)
        .bind("p2").to("singerName").build();

    final String concertInsertStatement = getInsertStatementForConcertsTable();
    final Statement concertInsertStatementWithValues = Statement.newBuilder(concertInsertStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(1L)
        .bind("p2").to(1L).build();

    // successful inserts into referenced and referencing tables
    databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(singerInsertStatementWithValues);
              return null;
            });
    databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(concertInsertStatementWithValues);
              return null;
            });

    final String singerIdColumnName = getSingerIdColumnName();
    final String singerFirstNameColumnName = getSingerFirstNameColumnName();
    final String concertVenueIdColumnName = getConcertVenueIdColumnName();

    try (ResultSet resultSet =
        databaseClient.singleUse().executeQuery(Statement.of("SELECT * FROM " + TABLE_NAME_SINGER))) {

      resultSet.next();
      assertEquals(1, resultSet.getLong(singerIdColumnName));
      assertEquals("singerName", resultSet.getString(singerFirstNameColumnName));

      assertThat(resultSet.next()).isFalse();
    }

    try (ResultSet resultSet =
        databaseClient.singleUse().executeQuery(Statement.of("SELECT * FROM " + TABLE_NAME_CONCERTS))) {

      resultSet.next();
      assertEquals(1, resultSet.getLong(singerIdColumnName));
      assertEquals(1, resultSet.getLong(concertVenueIdColumnName));

      assertThat(resultSet.next()).isFalse();
    }
  }

  @Test
  public void testForeignKeyDeleteCascadeConstraints_verifyInvalidInsertions() {
    final List<String> createStatements = getCreateTableStatementsWithForeignKey();
    final Database createdDatabase = createDatabase(createStatements);
    dbs.add(createdDatabase);

    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);

    // unsuccessful inserts into referencing tables when foreign key is not inserted into referenced table
    final String concertInsertStatement = getInsertStatementForConcertsTable();
    final Statement concertInsertStatementWithInvalidValues = Statement.newBuilder(concertInsertStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(2L)
        .bind("p2").to(2L).build();
    try {
      databaseClient
          .readWriteTransaction()
          .run(
              transaction -> {
                transaction.executeUpdate(concertInsertStatementWithInvalidValues);
                return null;
              });
      fail("Expected exception");
    } catch (SpannerException ex) {
      assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.FAILED_PRECONDITION);
      assertThat(ex.getMessage()).contains("Cannot find referenced values");
    }
  }
  @Test
  public void testForeignKeyDeleteCascadeConstraints_forDeletions() {
    final List<String> createStatements = getCreateTableStatementsWithForeignKey();
    final Database createdDatabase = createDatabase(createStatements);
    dbs.add(createdDatabase);

    final DatabaseClient databaseClient = env.getTestHelper().getDatabaseClient(createdDatabase);

    final String singerInsertStatement = getInsertStatementForSingerTable();
    final Statement singerInsertStatementWithValues = Statement.newBuilder(singerInsertStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(1L)
        .bind("p2").to("singerName").build();

    final String concertInsertStatement = getInsertStatementForConcertsTable();
    final Statement concertInsertStatementWithValues = Statement.newBuilder(concertInsertStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(1L)
        .bind("p2").to(1L).build();

    // successful inserts into referenced and referencing tables
    databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(singerInsertStatementWithValues);
              return null;
            });
    databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(concertInsertStatementWithValues);
              return null;
            });

    // execute delete
    final String singerDeleteStatement = getDeleteStatementForSingerTable();
    final Statement singerDeleteStatementWithValues = Statement.newBuilder(singerDeleteStatement)
        // Use 'p1' to bind to the parameter with index 1 etc.
        .bind("p1").to(1L).build();
    databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(singerDeleteStatementWithValues);
              return null;
            });

    try (ResultSet resultSet =
        databaseClient.singleUse().executeQuery(Statement.of("SELECT * FROM " + TABLE_NAME_SINGER))) {
      assertThat(resultSet.next()).isFalse();
    }

    try (ResultSet resultSet =
        databaseClient.singleUse().executeQuery(Statement.of("SELECT * FROM " + TABLE_NAME_CONCERTS))) {
      assertThat(resultSet.next()).isFalse();
    }
  }
  private Database createDatabase(final List<String> statements) {
    final Database database;
    if (dialect.dialect == Dialect.POSTGRESQL) {
      database = env.getTestHelper()
          .createTestDatabase(Dialect.POSTGRESQL, statements);
    } else {
      database = env.getTestHelper()
          .createTestDatabase(statements);
    }
    return database;
  }

  private List<String> getCreateTableStatementsWithForeignKey() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return ImmutableList.of(POSTGRES_CREATE_TABLE_SINGER, POSTGRES_CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY);
    } else {
      return ImmutableList.of(CREATE_TABLE_SINGER, CREATE_TABLE_CONCERT_WITH_FOREIGN_KEY);
    }
  }

  private String getReferentialConstraintsQueryStatement() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return POSTGRES_QUERY_REFERENTIAL_CONSTRAINTS;
    } else {
      return QUERY_REFERENTIAL_CONSTRAINTS;
    }
  }

  private List<String> getCreateAndAlterTableStatementsWithForeignKey() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return ImmutableList.of(POSTGRES_CREATE_TABLE_SINGER, POSTGRES_CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY,
          POSTGRES_ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY);
    } else {
      return ImmutableList.of(CREATE_TABLE_SINGER, CREATE_TABLE_CONCERT_WITHOUT_FOREIGN_KEY,
          ALTER_TABLE_CONCERT_WITH_FOREIGN_KEY);
    }
  }

  private List<String> getAlterDropForeignKeyDeleteCascadeStatements() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return ImmutableList.of(ALTER_TABLE_CONCERT_DROP_FOREIGN_KEY_CONSTRAINT,
          POSTGRES_ALTER_TABLE_CONCERT_UPDATE_FOREIGN_KEY_WITHOUT_DELETE_CASCADE);
    } else {
      return ImmutableList.of(ALTER_TABLE_CONCERT_DROP_FOREIGN_KEY_CONSTRAINT,
          ALTER_TABLE_CONCERT_UPDATE_FOREIGN_KEY_WITHOUT_DELETE_CASCADE);
    }
  }

  private String getInsertStatementForSingerTable() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "INSERT INTO Singer (singer_id, first_name) VALUES ($1, $2)";
    } else {
      return "INSERT INTO Singer (SingerId, FirstName) VALUES ($1, $2)";
    }
  }

  private String getInsertStatementForConcertsTable() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "INSERT INTO Concerts (venue_id, singer_id) VALUES ($1, $2)";
    } else {
      return "INSERT INTO Concerts (VenueId, SingerId) VALUES ($1, $2)";
    }
  }

  private String getDeleteStatementForSingerTable() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "DELETE FROM Singer WHERE singer_id = $1";
    } else {
      return "DELETE FROM Singer WHERE SingerId = $1";
    }
  }

  private String getConcertVenueIdColumnName() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "venue_id";
    } else {
      return "VenueId";
    }
  }

  private String getSingerFirstNameColumnName() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "first_name";
    } else {
      return "FirstName";
    }
  }

  private String getSingerIdColumnName() {
    if (dialect.dialect == Dialect.POSTGRESQL) {
      return "singer_id";
    } else {
      return "SingerId";
    }
  }
}
