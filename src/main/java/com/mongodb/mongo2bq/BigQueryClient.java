package com.mongodb.mongo2bq;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.BidiStream;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class BigQueryClient {
    
    private static final Logger logger = LoggerFactory.getLogger(BigQueryClient.class);
    
    // Number of documents to sample for schema inference
    private static final int SCHEMA_SAMPLE_SIZE = 10;
    
    private MongoToBigQueryConfig config;
    
    private WriteStream currentStream;
    private String currentStreamName;
    private final AtomicLong rowsWritten = new AtomicLong(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private final StopWatch streamStopWatch = new StopWatch();
    
    private String tableName;
    private String parentTable;
    
    public BigQueryClient(MongoToBigQueryConfig config) {
		this.config = config;
	}

	public static boolean tableExists(BigQuery bigQuery, TableId tableId) {
        return bigQuery.getTable(tableId) != null;
    }
    
	private static boolean isTableFullyAvailable(String projectId, String datasetId, String tableId) {
	    try {
	        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
	        Table table = bigQuery.getTable(TableId.of(projectId, datasetId, tableId));
	        
	        if (table == null) {
	            return false;
	        }
	        
	        // Try to get schema - this will fail if table is not ready
	        Schema schema = table.getDefinition().getSchema();
	        return schema != null;
	    } catch (Exception e) {
	        logger.warn("Error checking table availability: {}", e.getMessage());
	        return false;
	    }
	}
	
	public static boolean waitForTableFullyAvailable(MongoToBigQueryConfig config, TableId tableId) {
		// Add this block after table creation
        logger.info("Waiting for new table to be fully available...");
        String fullyQualifiedTableId = String.format("%s.%s.%s", 
                config.getGcpProjectId(), config.getBqDatasetName(), tableId.getTable());
        
        boolean tableReady = false;
        int retryCount = 0;
        int maxRetries = 10;
        long retryDelayMs = 5000; // 5 seconds
        
        while (!tableReady && retryCount < maxRetries) {
            logger.info("Verifying table exists: {}", fullyQualifiedTableId);
            tableReady = isTableFullyAvailable(config.getGcpProjectId(), 
                                             config.getBqDatasetName(), 
                                             tableId.getTable());
            if (!tableReady) {
                retryCount++;
                if (retryCount < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for table to be available", e);
                    }
                } else {
                    logger.error("Table not available after {} attempts", maxRetries);
                }
            }
        }
        return tableReady;
	}
	
    /**
     * Initialize a new write stream
     * @param parentTable2 
     */
	public void initializeStream(BigQueryWriteClient client, String tableName, String parentTable) {
	    int maxRetries = 5;
	    int retryDelayMs = 1000;
	    
	    this.tableName = tableName;
	    this.parentTable = parentTable;
	    
	    for (int attempt = 1; attempt <= maxRetries; attempt++) {
	        try {
	            // Create a write stream for the specified table
	            WriteStream stream = WriteStream.newBuilder()
	                .setType(WriteStream.Type.PENDING)
	                .build();
	            
	            CreateWriteStreamRequest createRequest = CreateWriteStreamRequest.newBuilder()
	                .setParent(parentTable)
	                .setWriteStream(stream)
	                .build();
	            
	            currentStream = client.createWriteStream(createRequest);
	            currentStreamName = currentStream.getName();
	            logger.info("Created new stream: {}", currentStreamName);
	            
	            // Reset counters
	            rowsWritten.set(0);
	            bytesWritten.set(0);
	            streamStopWatch.reset();
	            streamStopWatch.start();
	            
	            return; // Success - exit method
	            
	        } catch (NotFoundException e) {
	            logger.warn("Table not found on attempt {}/{}. Retrying in {} ms", 
	                      attempt, maxRetries, retryDelayMs);
	            
	            if (attempt == maxRetries) {
	                throw new RuntimeException("Failed to create write stream after " + maxRetries + " attempts", e);
	            }
	            
	            try {
	                Thread.sleep(retryDelayMs);
	                retryDelayMs *= 2; // Exponential backoff
	            } catch (InterruptedException ie) {
	                Thread.currentThread().interrupt();
	                throw new RuntimeException("Interrupted during retry", ie);
	            }
	        }
	    }
	}
	
    /**
     * Check if we need to rotate the stream based on thresholds
     */
    private String shouldRotateStream() {
        if (rowsWritten.get() >= config.getMaxRowsPerStream()) {
            return "row count threshold reached";
        } else if (streamStopWatch.getTime() >= config.getMaxStreamDurationMinutes() * 60000L) {
            return "time threshold reached";
        } else if (bytesWritten.get() >= config.getMaxMegabytesPerStream() * 1024 * 1024) {
            return "byte threshold reached";
        }
        return null;
    }
    
    /**
     * Finalize the current stream and create a new one
     */
    public void rotateStreamIfNeeded(BigQueryWriteClient client) {
        String rotateReason = shouldRotateStream();
        if (rotateReason != null) {
            logger.info("Rotating stream due to {}: rows={}, bytes={}, elapsed={} ms",
                        rotateReason, rowsWritten.get(), bytesWritten.get(), streamStopWatch.getTime());
            
            // Finalize current stream
            finalizeAndCommitStream(client, parentTable);
            
            // Create a new stream
            initializeStream(client, tableName, parentTable);
        }
    }
    
    /**
     * Finalize and commit a stream
     */
    public void finalizeAndCommitStream(BigQueryWriteClient client, String parentTable) {
        try {
        	
            logger.info("Finalizing stream: {}", currentStreamName);
            
            // Send finalize request
            FinalizeWriteStreamRequest finalizeRequest = FinalizeWriteStreamRequest.newBuilder()
                .setName(currentStreamName)
                .build();
            
            FinalizeWriteStreamResponse finalizeResponse = client.finalizeWriteStream(finalizeRequest);
            logger.info("Stream finalized: {}, row count: {}", currentStreamName, finalizeResponse.getRowCount());
            
            // Commit the stream
            BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(parentTable)
                .addWriteStreams(currentStreamName)
                .build();
            
            BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
            
            // Check for errors
            List<StorageError> errors = commitResponse.getStreamErrorsList();
            for (StorageError e : errors) {
                logger.error(e.getErrorMessage());
            }
            
            if (commitResponse.hasCommitTime()) {
                logger.info("Stream committed successfully at {}", commitResponse.getCommitTime());
            } else {
                logger.error("Commit failed for stream {}", currentStreamName);
            }
        } catch (Exception e) {
            logger.error("Error finalizing/committing stream: {}", e.getMessage(), e);
        }
    }
    
	public void convertAndSendBatch(List<Document> batch, ProtoSchemaConverter converter)
	        throws IOException {
	    
	    // Get allowed fields from BigQuery table
	    Set<String> allowedFields = BigQueryClient.fetchBigQueryTableFields(config.getGcpProjectId(), config.getBqDatasetName(), tableName);

	    // Add flag to track if schema was updated
	    boolean schemaUpdated = false;
	    
	    AppendRowsRequest request = converter.createAppendRequest(currentStreamName, batch, tableName, allowedFields);
	            
	    if (request == null) {
	        logger.warn("No rows to append after conversion");
	        return;
	    }
	    
	    BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream = null;;
	    try {
	        // Create a new BidiStream for each batch
	        bidiStream = config.getBigQueryClient().appendRowsCallable()
	                .call();
	        
	        // Send the request
	        bidiStream.send(request);
	        
	        // Get the first response
	        AppendRowsResponse response = null;
	        Iterator<AppendRowsResponse> responseIter = bidiStream.iterator();
	        if (responseIter.hasNext()) {
	            response = responseIter.next();
	        }
	        
	        // Process the response
	        if (response != null && response.hasError()) {
	            logger.error("Error in append response: {}", response.getError().getMessage());
	            throw new IOException("Error appending rows: " + response.getError().getMessage());
	        } else if (response != null && response.hasAppendResult()) {
	            long estimatedBytes = request.getProtoRows().getSerializedSize();
	            int rowCount = request.getProtoRows().getRows().getSerializedRowsCount();
	            
	            // Update counters for stream rotation logic
	            rowsWritten.addAndGet(rowCount);
	            bytesWritten.addAndGet(estimatedBytes);
	            
	            logger.info("Successfully appended batch with offset: {}",
	                    response.getAppendResult().getOffset().getValue());
	        } else {
	            logger.warn("Received empty or incomplete response");
	        }
	    } catch (InvalidArgumentException e) {
	        // Check if error is about schema mismatch
	        String errorMessage = e.getMessage();
	        if (errorMessage.contains("Input schema has more fields than BigQuery schema")) {
	            logger.warn("Schema mismatch detected. Waiting for schema changes to propagate...");
	            try {
	                // Wait a bit longer for schema changes to fully propagate
	                Thread.sleep(10000); // 10 seconds
	                
	                // Re-fetch allowed fields after waiting
	                allowedFields = BigQueryClient.fetchBigQueryTableFields(config.getGcpProjectId(), config.getBqDatasetName(), tableName);
	                
	                // Try creating a new request with refreshed schema
	                request = converter.createAppendRequest(currentStreamName, batch, tableName, allowedFields);
	                
	                if (request == null) {
	                    throw new IOException("Failed to create request after schema update");
	                }
	                
	                // Create new BidiStream and retry
	                BidiStream<AppendRowsRequest, AppendRowsResponse> retryStream = 
	                        config.getBigQueryClient().appendRowsCallable().call();
	                
	                logger.info("Retrying batch with updated schema...");
	                retryStream.send(request);
	                
	                // Process response
	                Iterator<AppendRowsResponse> retryIter = retryStream.iterator();
	                if (retryIter.hasNext()) {
	                    AppendRowsResponse retryResponse = retryIter.next();
	                    if (retryResponse.hasError()) {
	                        throw new IOException("Error on retry: " + retryResponse.getError().getMessage());
	                    } else {
	                        logger.info("Successfully sent batch after schema update");
	                    }
	                }
	                
	                // Close the retry stream
	                retryStream.closeSend();
	                
	            } catch (InterruptedException ie) {
	                Thread.currentThread().interrupt();
	                throw new IOException("Interrupted while waiting for schema update", ie);
	            }
	        } else {
	            // If it's some other error, rethrow
	            logger.error("Error while sending batch", e);
	            throw new IOException("Error sending batch: " + e.getMessage(), e);
	        }
	    } finally {
	        // Always close the BidiStream when done
	        try {
	        	if (bidiStream != null) {
	        		bidiStream.closeSend();
	        	}
	        } catch (Exception e) {
	            logger.warn("Error closing BidiStream: {}", e.getMessage());
	        }
	    }
	}
     
	public static BigQueryWriteClient createBigQueryClient(MongoToBigQueryConfig config) throws IOException {
		String serviceAccountKeyPath = config.getServiceAccountKeyPath();
		GoogleCredentials credentials;
	    if (serviceAccountKeyPath != null && !serviceAccountKeyPath.isEmpty()) {
	        // Use explicit service account credentials if provided
	        try (FileInputStream serviceAccountStream = new FileInputStream(serviceAccountKeyPath)) {
	            credentials = GoogleCredentials.fromStream(serviceAccountStream);
	            logger.info("Using service account credentials from: {}", serviceAccountKeyPath);
	        }
	    } else {
	        // Fall back to application default credentials
	        credentials = GoogleCredentials.getApplicationDefault();
	        logger.info("Using application default credentials");
	    }
	    
		BigQueryWriteSettings writeSettings = BigQueryWriteSettings.newBuilder()
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
		return BigQueryWriteClient.create(writeSettings);
	}
    
    /**
     * Fetch allowed field names from BigQuery table
     */
    public static Set<String> fetchBigQueryTableFields(String projectId, String datasetId, String tableId) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableIdObj = TableId.of(projectId, datasetId, tableId);
            Table table = bigquery.getTable(tableIdObj);
            
            if (table == null) {
                logger.error("Table not found: {}", tableIdObj);
                return null;
            }
            
            Schema schema = table.getDefinition().getSchema();
            if (schema == null) {
                logger.error("Schema is null for table: {}", tableIdObj);
                return null;
            }
            
            Set<String> fieldNames = new HashSet<>();
            for (Field field : schema.getFields()) {
                fieldNames.add(field.getName().toLowerCase());
            }
            
            logger.info("Fetched {} fields from BigQuery table {}", fieldNames.size(), tableIdObj);
            return fieldNames;
        } catch (Exception e) {
            logger.error("Error fetching BigQuery table schema: {}", e.getMessage(), e);
            return null;
        }
    }
    
    public static void createBigQueryTable(BigQuery bigQuery, MongoCollection<Document> collection, TableId tableId) {
        Map<String, Field> uniqueFields = new LinkedHashMap<>(); // Preserves insertion order and ensures uniqueness

        // Sample a few documents to infer schema
        FindIterable<Document> sampleDocs = collection.find().limit(SCHEMA_SAMPLE_SIZE);
        for (Document doc : sampleDocs) {
            for (String key : doc.keySet()) {
                uniqueFields.putIfAbsent(key, Field.of(key, inferBigQueryType(doc.get(key))));
            }
        }

        Schema schema = Schema.of(new ArrayList<>(uniqueFields.values()));
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
        logger.info("Created table: {}", tableId.getTable());
    }
    
    /**
     * Get the set of fields in a BigQuery table
     */
    public static Set<String> getBigQueryFields(String projectId, String datasetId, String tableId) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            Table table = bigquery.getTable(TableId.of(projectId, datasetId, tableId));
            
            if (table == null) {
                logger.error("Table not found: {}.{}.{}", projectId, datasetId, tableId);
                return null;
            }
            
            Set<String> fieldNames = new HashSet<>();
            for (Field field : table.getDefinition().getSchema().getFields()) {
                // Keep original case but still use case-insensitive comparisons elsewhere
                fieldNames.add(field.getName());
            }
            
            return fieldNames;
        } catch (Exception e) {
            logger.error("Error fetching BigQuery field names: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Convert BigQuery type to Protobuf type
     */
    public static Type convertBigQueryTypeToProtoType(LegacySQLTypeName bqType) {
        if (bqType == LegacySQLTypeName.INTEGER) {
            return Type.TYPE_INT64;
        } else if (bqType == LegacySQLTypeName.FLOAT || bqType == LegacySQLTypeName.NUMERIC) {
            return Type.TYPE_DOUBLE;
        } else if (bqType == LegacySQLTypeName.BOOLEAN) {
            return Type.TYPE_BOOL;
        } else if (bqType == LegacySQLTypeName.TIMESTAMP || bqType == LegacySQLTypeName.DATETIME) {
            return Type.TYPE_INT64; // Use int64 for timestamps
        } else if (bqType == LegacySQLTypeName.DATE) {
            return Type.TYPE_STRING;
        } else if (bqType == LegacySQLTypeName.BYTES) {
            return Type.TYPE_BYTES;
        } else {
            // STRING, TIME, RECORD and default cases
            return Type.TYPE_STRING;
        }
    }
    
    /**
     * Get field types from a BigQuery table
     */
    public static Map<String, LegacySQLTypeName> getBigQueryFieldTypes(String projectId, String datasetId, String tableId) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableIdObj = TableId.of(projectId, datasetId, tableId);
            Table table = bigquery.getTable(tableIdObj);
            
            if (table == null) {
                logger.error("Table not found: {}", tableIdObj);
                return null;
            }
            
            Map<String, LegacySQLTypeName> fieldTypes = new HashMap<>();
            for (Field field : table.getDefinition().getSchema().getFields()) {
                fieldTypes.put(field.getName().toLowerCase(), field.getType());
            }
            
            logger.info("Fetched schema with {} fields from BigQuery table {}", fieldTypes.size(), tableIdObj);
            return fieldTypes;
        } catch (Exception e) {
            logger.error("Error fetching BigQuery schema: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Infer BigQuery field type from MongoDB data
     */
    public static StandardSQLTypeName inferBigQueryFieldType(String fieldName, List<Document> documents) {
        // Default to STRING if we can't determine a better type
    	StandardSQLTypeName defaultType = StandardSQLTypeName.STRING;
        
        for (Document doc : documents) {
            if (doc.containsKey(fieldName)) {
                Object value = doc.get(fieldName);
                if (value == null) continue;
                
                if (value instanceof Integer) return StandardSQLTypeName.INT64;
                if (value instanceof Long) return StandardSQLTypeName.INT64;
                if (value instanceof Double) return StandardSQLTypeName.FLOAT64;
                if (value instanceof Float) return StandardSQLTypeName.FLOAT64;
                if (value instanceof Boolean) return StandardSQLTypeName.BOOL;
                if (value instanceof Date) return StandardSQLTypeName.TIMESTAMP;
                // For complex types, default to STRING
            }
        }
        
        return defaultType;
    }
    
    /**
     * Infers BigQuery column type from MongoDB field value
     */
    private static StandardSQLTypeName inferBigQueryType(Object value) {
        if (value == null) {
            return StandardSQLTypeName.STRING; // Default to STRING for null values
        } else if (value instanceof Integer || value instanceof Long) {
            return StandardSQLTypeName.INT64;
        } else if (value instanceof Double || value instanceof Float) {
            return StandardSQLTypeName.FLOAT64;
        } else if (value instanceof Boolean) {
            return StandardSQLTypeName.BOOL;
        } else if (value instanceof java.util.Date || value instanceof java.sql.Timestamp) {
            return StandardSQLTypeName.TIMESTAMP;
        } else if (value instanceof List) {
            // For lists, we need to determine element type
            List<?> list = (List<?>) value;
            if (!list.isEmpty()) {
                Object firstElement = list.get(0);
                if (firstElement instanceof Document) {
                    // Nested record in array
                    return StandardSQLTypeName.STRING; // Convert to JSON string for now
                } else if (list.get(0) instanceof String) {
                    return StandardSQLTypeName.STRING; // Array of strings
                } else if (list.get(0) instanceof Number) {
                    return StandardSQLTypeName.FLOAT64; // Array of numbers
                }
            }
            return StandardSQLTypeName.STRING; // Default for empty lists
        } else if (value instanceof Document) {
            // Nested document
            return StandardSQLTypeName.STRING; // Convert to JSON string for now
        } else {
            return StandardSQLTypeName.STRING; // Default to STRING for other types
        }
    }
}