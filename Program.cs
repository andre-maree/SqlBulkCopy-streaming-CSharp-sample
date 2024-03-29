﻿using System.Data;
using System.Data.SqlClient;

class Program
{
    static async Task Main()
    {
        string tableName = "Customers";
        string soutceConnectionString = "Integrated Security=SSPI;Persist Security Info=False;User ID=admin;Initial Catalog=MyDB;Data Source=.";
        string targetConnectionString = "Integrated Security=SSPI;Persist Security Info=False;User ID=admin;Initial Catalog=MyDB2;Data Source=.";

        // 1st create 2 databases MyDB and MyDB2
        // include this line to set up the tables on the 1st run of this code
        await CreateTablesAndPopulateSource(tableName, soutceConnectionString, targetConnectionString);

        await SyncTables(tableName, soutceConnectionString, targetConnectionString);
    }

    static async Task SyncTables(string tableName, string sourceConnection, string targetConnection)
    {
        int timeout = 999;

        // Open a sourceConnection to the MyDB database.
        using SqlConnection sourceCon = new(sourceConnection);

        await sourceCon.OpenAsync();

        SqlDataReader reader;

        using (SqlCommand cmd = new($"SELECT * FROM {tableName}", sourceCon))
        {
            reader = await cmd.ExecuteReaderAsync();
        }

        using SqlConnection destinationConnection = new(targetConnection);

        // check for a possible config issue, source and target should not be the same table
        if (sourceCon.DataSource.Equals(destinationConnection.DataSource, StringComparison.OrdinalIgnoreCase)
            && sourceCon.Database.Equals(destinationConnection.Database, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("The source and destination tables is the same table on the same sql instance.");
        }

        await destinationConnection.OpenAsync();

        using SqlTransaction transaction = destinationConnection.BeginTransaction();

        using (SqlCommand cmd = new($"truncate table {tableName}", destinationConnection, transaction))
        {
            cmd.CommandTimeout = timeout;
            await cmd.ExecuteNonQueryAsync();
        }

        using SqlBulkCopy bulkCopy = new(destinationConnection, SqlBulkCopyOptions.KeepIdentity, transaction);

        bulkCopy.EnableStreaming = true;
        bulkCopy.DestinationTableName = tableName;
        bulkCopy.BulkCopyTimeout = timeout;

        try
        {
            // Write from the source to the destination.
            await bulkCopy.WriteToServerAsync(reader);
            await transaction.CommitAsync();
        }
        catch (Exception)
        {
            transaction.Rollback();
        }
        finally
        {
            // Close the SqlDataReader
            await reader.CloseAsync();
            await sourceCon.CloseAsync();
        }
    }

    private static async Task CreateTablesAndPopulateSource(string tableName, string soutceConnectionString, string targetConnectionString)
    {
        string cmdText = $@"CREATE TABLE [dbo].[{tableName}] (
                                  [Id][uniqueidentifier] ROWGUIDCOL NOT NULL
                                   CONSTRAINT[PK_Customers] PRIMARY KEY CLUSTERED,
                                  [LastName] [nvarchar](255) NULL,
                                  [FirstName] [nvarchar](255) NULL,
                                  [Street] [nvarchar](255) NULL,
                                  [City] [nvarchar](255) NULL,
                                  [State] [nvarchar](255) NULL,
                                  [PhoneNumber] [nvarchar](255) NULL,
                                  [EmailAddress] [nvarchar](255) NULL
                                )";

        using (SqlConnection sourceCon = new(soutceConnectionString))
        {
            var command = new SqlCommand(cmdText, sourceCon);

            await sourceCon.OpenAsync();

            await command.ExecuteNonQueryAsync();

            command.CommandText = $"SELECT * FROM [{tableName}]";
            SqlDataAdapter adapter = new(command);

            DataTable dt = new();

            await Task.Run(() => adapter.Fill(dt));

            for (int i = 0; i < 100000; i++)
            {
                dt.Rows.Add(new object[] { Guid.NewGuid().ToString(), "f", "l", "s", "c", "st", 123, "asd" });
            }

            using var copy = new SqlBulkCopy(sourceCon);

            copy.DestinationTableName = tableName;

            await copy.WriteToServerAsync(dt);

            await sourceCon.CloseAsync();
        }

        using (SqlConnection targetConnection = new(targetConnectionString))
        {
            var command = new SqlCommand(cmdText, targetConnection);

            await targetConnection.OpenAsync();

            await command.ExecuteNonQueryAsync();

            await targetConnection.CloseAsync();
        }
    }
}