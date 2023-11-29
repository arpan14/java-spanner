package com.example.spanner;

//[START spanner_create_database_with_default_leader]

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import com.google.spanner.admin.database.v1.Database;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class CreateDatabaseAutogenerationSample {

  static void createDatabaseWithDefaultLeader() throws IOException {
    // TODO(developer): Replace these variables before running the sample.
    final String projectId = "my-project";
    final String instanceId = "my-instance";
    final String databaseId = "my-database";
    final String defaultLeader = "my-default-leader";
    createDatabaseWithDefaultLeader(projectId, instanceId, databaseId, defaultLeader);
  }

  static void createDatabaseWithDefaultLeader(
      String projectId, String instanceId, String databaseId, String defaultLeader) throws IOException {
    DatabaseAdminClient databaseAdminClient = DatabaseAdminClient.create();

    try {
      OperationFuture<Database, CreateDatabaseMetadata> op2 =
          databaseAdminClient.createDatabaseAsync(
              CreateDatabaseRequest.newBuilder()
                  .setParent(instanceId)
                  .setCreateStatement("CREATE DATABASE " + databaseId)
                  .addAllExtraStatements(
                      ImmutableList.of("CREATE TABLE Singers ("
                              + "  SingerId   INT64 NOT NULL,"
                              + "  FirstName  STRING(1024),"
                              + "  LastName   STRING(1024),"
                              + "  SingerInfo BYTES(MAX)"
                              + ") PRIMARY KEY (SingerId)",
                          "CREATE TABLE Albums ("
                              + "  SingerId     INT64 NOT NULL,"
                              + "  AlbumId      INT64 NOT NULL,"
                              + "  AlbumTitle   STRING(MAX)"
                              + ") PRIMARY KEY (SingerId, AlbumId),"
                              + "  INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
                          "ALTER DATABASE " + "`" + databaseId + "`"
                              + " SET OPTIONS ( default_leader = '" + defaultLeader + "' )"))
                  .build());
      Database createdDatabase = op2.get();
      System.out.println("Created database [" + createdDatabase.getName() + "]");
      System.out.println("\tDefault leader: " + createdDatabase.getDefaultLeader());
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }
}
//[END spanner_create_database_with_default_leader]