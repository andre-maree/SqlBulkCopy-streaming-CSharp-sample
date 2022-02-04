# SqlBulkCopy-streaming-sample
Sync 2 sql tables using SqlBulkCopy data streaming:
- this sample code sets up 2 tables
- populates the source table with data
- opens a DataReader
- sets EnableStreaming = true
- SqlBulkCopies the source data to the target table via streaming
