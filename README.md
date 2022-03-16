# SqlBulkCopy-streaming-c#-sample - .NET 6 Console App
Sync 2 sql tables using SqlBulkCopy data streaming:
- this sample code sets up 2 tables, only run this method in the 1st run, then uncomment it
- populates the source table with data
- opens a DataReader
- sets EnableStreaming = true
- SqlBulkCopies the source data to the target table via streaming
