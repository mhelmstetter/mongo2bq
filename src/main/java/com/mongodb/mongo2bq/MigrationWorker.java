package com.mongodb.mongo2bq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.BidiStream;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MigrationWorker implements Runnable {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MigrationWorker.class);

	private MongoToBigQueryConfig config;
	private MongoClient mongoClient;
	private Namespace ns;
	private Document collectionInfo;
	private ProtoSchemaConverter converter;
	private boolean stopRequested = false;
    
    // Stream management variables
    private WriteStream currentStream;
    //private BidiStream<AppendRowsRequest, AppendRowsResponse> currentBidiStream;
    private String currentStreamName;
    private final AtomicLong rowsWritten = new AtomicLong(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private final StopWatch streamStopWatch = new StopWatch();

	public MigrationWorker(MongoToBigQueryConfig config, String mongoClientName, Namespace ns,
			Document collectionInfo) {
		this.config = config;
		this.mongoClient = config.getMongoClient(mongoClientName);
		this.ns = ns;
		this.collectionInfo = collectionInfo;
		converter = new ProtoSchemaConverter(config);
	}

	@Override
	public void run() {
	    TableId tableId = TableId.of(config.getBqDatasetName(), ns.getDatabaseName() + "_" + ns.getCollectionName());
	    MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());

	    if (!BigQueryHelper.tableExists(config.getBigQuery(), tableId)) {
	        BigQueryHelper.createBigQueryTable(config.getBigQuery(), db.getCollection(ns.getCollectionName()), tableId);
	        
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
	                    return; // Exit the run method
	                }
	            }
	        }
	    } else {
	        logger.debug("BigQuery table already exists: {}", tableId);
	    }
	    
	    // Only proceed to process the collection if we have a table
	    processCollection(config.getBigQueryClient(), db.getCollection(ns.getCollectionName()), 
	                    ns.getDatabaseName(), collectionInfo);
	}
	
	private boolean isTableFullyAvailable(String projectId, String datasetId, String tableId) {
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

	private void processCollection(BigQueryWriteClient client, MongoCollection<Document> collection, String dbName,
			Document collectionInfo) {
		
		String collectionName = collectionInfo.getString("name");
		try {
			String collectionType = collectionInfo.getString("type");
			String tableName = dbName + "_" + collectionName;
			String parentTable = TableName.of(config.getGcpProjectId(), config.getBqDatasetName(), tableName)
					.toString();

            // Initialize stream management
            initializeStream(client, parentTable);

			logger.info("Processing MongoDB Collection: {}.{}", dbName, collectionName);

			if ("collection".equals(collectionType)) {
				processRegularCollection(collection, currentStreamName, dbName, collectionName, tableName, parentTable, client);
			} else if ("timeseries".equals(collectionType)) {
				processTimeSeriesCollection(collection, collectionInfo, currentStreamName, dbName, collectionName, tableName, parentTable, client);
			} else {
				logger.debug("Skipping collection type: {}, name: {}", collectionType, collectionName);
				return;
			}

            // Final cleanup - finalize current stream if it exists
            if (currentStreamName != null) {
                finalizeAndCommitStream(client, currentStreamName, parentTable);
            }

		} catch (Exception e) {
			logger.error("Error processing collection {}.{}", dbName, collectionName, e);
		}
	}
    
    /**
     * Initialize a new write stream
     */
	private void initializeStream(BigQueryWriteClient client, String parentTable) {
	    int maxRetries = 5;
	    int retryDelayMs = 1000;
	    
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
    private void rotateStreamIfNeeded(BigQueryWriteClient client, String parentTable) {
        String rotateReason = shouldRotateStream();
        if (rotateReason != null) {
            logger.info("Rotating stream due to {}: rows={}, bytes={}, elapsed={} ms",
                        rotateReason, rowsWritten.get(), bytesWritten.get(), streamStopWatch.getTime());
            
            // Finalize current stream
            finalizeAndCommitStream(client, currentStreamName, parentTable);
            
            // Create a new stream
            initializeStream(client, parentTable);
        }
    }
    
    /**
     * Finalize and commit a stream
     */
    private void finalizeAndCommitStream(BigQueryWriteClient client, String streamName, String parentTable) {
        try {
        	
            logger.info("Finalizing stream: {}", streamName);
            
            // Send finalize request
            FinalizeWriteStreamRequest finalizeRequest = FinalizeWriteStreamRequest.newBuilder()
                .setName(streamName)
                .build();
            
            FinalizeWriteStreamResponse finalizeResponse = client.finalizeWriteStream(finalizeRequest);
            logger.info("Stream finalized: {}, row count: {}", streamName, finalizeResponse.getRowCount());
            
            // Commit the stream
            BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(parentTable)
                .addWriteStreams(streamName)
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
                logger.error("Commit failed for stream {}", streamName);
            }
        } catch (Exception e) {
            logger.error("Error finalizing/committing stream: {}", e.getMessage(), e);
        }
    }

	private void processRegularCollection(MongoCollection<Document> collection, String streamName, String dbName,
			String collectionName, String tableName, String parentTable, BigQueryWriteClient client) {
		// Start with initial sort on _id
		Document sort = new Document("_id", 1);
		Object lastId = null;
		boolean isFirstBatch = true;

		while (true) {
			// Build query - for subsequent batches, filter by _id > lastId
			FindIterable<Document> docs;
			if (isFirstBatch) {
				docs = collection.find().sort(sort).limit(config.getBatchSize());
				isFirstBatch = false;
			} else {
				docs = collection.find(new Document("_id", new Document("$gt", lastId))).sort(sort)
						.limit(config.getBatchSize());
			}

			// Process batch
			List<Document> batch = new ArrayList<>(config.getBatchSize());
			MongoCursor<Document> cursor = docs.iterator();
			int count = 0;

			while (cursor.hasNext()) {
				Document doc = cursor.next();
				batch.add(doc);
				lastId = doc.get("_id");
				count++;
			}

			// If we got no documents, we're done
			if (count == 0) {
				break;
			}

			// Send batch to BigQuery
			logger.info("Sending batch of {} documents from {}.{} (regular collection)", batch.size(), dbName, collectionName);
			try {
				convertAndSendBatch(batch, streamName, tableName);
                
                // Check if we need to rotate the stream
                rotateStreamIfNeeded(client, parentTable);
                
                // Update stream name after potential rotation
                streamName = currentStreamName;
			} catch (IOException e) {
				logger.error("Error sending batch", e);
			}

			// If we got fewer documents than config.getBatchSize(), we're done
			if (count < config.getBatchSize()) {
				break;
			}
		}
	}


	private void convertAndSendBatch(List<Document> batch, String streamName, String tableName)
	        throws IOException {
	    
	    // Get allowed fields from BigQuery table
	    Set<String> allowedFields = BigQueryHelper.fetchBigQueryTableFields(config.getGcpProjectId(), config.getBqDatasetName(), tableName);

	    // Add flag to track if schema was updated
	    boolean schemaUpdated = false;
	    
	    AppendRowsRequest request = converter.createAppendRequest(streamName, batch, tableName, allowedFields);
	            
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
	                allowedFields = BigQueryHelper.fetchBigQueryTableFields(config.getGcpProjectId(), config.getBqDatasetName(), tableName);
	                
	                // Try creating a new request with refreshed schema
	                request = converter.createAppendRequest(streamName, batch, tableName, allowedFields);
	                
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

	private void processTimeSeriesCollection(MongoCollection<Document> collection, Document collectionInfo,
			String streamName, String dbName, String collectionName, String tableName, String parentTable, BigQueryWriteClient client) {
		
		// Extract timeField from collection info
		Document options = (Document) collectionInfo.get("options");
		Document timeseries = (Document) options.get("timeseries");
		String timeField = timeseries.getString("timeField");

		// Sort by timeField
		Document sort = new Document(timeField, 1);
		Object lastTimeValue = null;
		boolean isFirstBatch = true;
		Document savedLookaheadDoc = null;

		while (true) {
			// Build query - for subsequent batches, filter by timeField > lastTimeValue
			FindIterable<Document> docs;
			if (isFirstBatch) {
				docs = collection.find().sort(sort);
				isFirstBatch = false;
			} else {
				Document query = new Document(timeField, new Document("$gt", lastTimeValue));
				docs = collection.find(query).sort(sort);
			}

			// Process batch
			List<Document> batch = new ArrayList<>(config.getBatchSize());

			// Add saved lookahead document from previous iteration if it exists
			if (savedLookaheadDoc != null) {
				batch.add(savedLookaheadDoc);
				savedLookaheadDoc = null;
			}

			MongoCursor<Document> cursor = docs.iterator();

			Object currentTimeValue = null;
			boolean batchComplete = false;
			int count = batch.size(); // Start with the count of any previously saved documents

			while (cursor.hasNext() && !batchComplete) {
				Document doc = cursor.next();
				currentTimeValue = doc.get(timeField);

				// Add document to batch
				batch.add(doc);
				count++;

				// Check if we've reached batch size
				if (count >= config.getBatchSize()) {
					// Look ahead to see if next document has same timeField value
					if (cursor.hasNext()) {
						Document nextDoc = cursor.next();
						Object nextTimeValue = nextDoc.get(timeField);

						// If next document has same timeField, include it and continue
						if (nextTimeValue.equals(currentTimeValue)) {
							batch.add(nextDoc);
							count++;

							// Continue looking ahead until timeField changes
							while (cursor.hasNext()) {
								Document anotherDoc = cursor.next();
								Object anotherTimeValue = anotherDoc.get(timeField);

								if (anotherTimeValue.equals(currentTimeValue)) {
									batch.add(anotherDoc);
									count++;
								} else {
									// Save document with different timeField for next batch
									savedLookaheadDoc = anotherDoc;
									batchComplete = true;
									break;
								}
							}
						} else {
							// Next document has different timeField, save it for next batch
							savedLookaheadDoc = nextDoc;
							batchComplete = true;
						}
					} else {
						batchComplete = true;
					}
				}

				// Remember the last time value for the next query
				lastTimeValue = currentTimeValue;
			}

			// If we got no new documents (and no saved document), we're done
			if (batch.isEmpty()) {
				break;
			}

			// Send batch to BigQuery
			logger.info(
					"Sending batch of {} documents from {}.{} (timeseries collection) with timeField range: {} to {}",
					batch.size(), dbName, collectionName, batch.get(0).get(timeField),
					batch.get(batch.size() - 1).get(timeField));
			try {
				convertAndSendBatch(batch, streamName, tableName);
                
                // Check if we need to rotate the stream
                rotateStreamIfNeeded(client, parentTable);
                
                // Update stream name after potential rotation
                streamName = currentStreamName;
			} catch (IOException e) {
				logger.error("Error sending batch", e);
			}

			// If cursor has no more documents and no lookahead saved, we're done
			if (!cursor.hasNext() && savedLookaheadDoc == null) {
				break;
			}
		}
	}
}