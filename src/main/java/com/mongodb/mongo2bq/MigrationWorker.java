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
	
	private BigQueryClient bigQueryClient;
    
	public MigrationWorker(MongoToBigQueryConfig config, String mongoClientName, Namespace ns,
			Document collectionInfo) {
		this.config = config;
		this.mongoClient = config.getMongoClient(mongoClientName);
		this.ns = ns;
		this.collectionInfo = collectionInfo;
		converter = new ProtoSchemaConverter(config);
		bigQueryClient = new BigQueryClient(config);
	}

	@Override
	public void run() {
	    TableId tableId = TableId.of(config.getBqDatasetName(), ns.getDatabaseName() + "_" + ns.getCollectionName());
	    MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());

	    if (!BigQueryClient.tableExists(config.getBigQuery(), tableId)) {
	        BigQueryClient.createBigQueryTable(config.getBigQuery(), db.getCollection(ns.getCollectionName()), tableId);
	        
	        BigQueryClient.waitForTableFullyAvailable(config, tableId);
	    } else {
	        logger.debug("BigQuery table already exists: {}", tableId);
	    }
	    
	    // Only proceed to process the collection if we have a table
	    processCollection(config.getBigQueryClient(), db.getCollection(ns.getCollectionName()), 
	                    ns.getDatabaseName(), collectionInfo);
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
            bigQueryClient.initializeStream(client, tableName, parentTable);

			logger.info("Processing MongoDB Collection: {}.{}", dbName, collectionName);

			if ("collection".equals(collectionType)) {
				processRegularCollection(collection, dbName, collectionName, client);
			} else if ("timeseries".equals(collectionType)) {
				processTimeSeriesCollection(collection, collectionInfo, dbName, collectionName, tableName, parentTable, client);
			} else {
				logger.debug("Skipping collection type: {}, name: {}", collectionType, collectionName);
				return;
			}

            bigQueryClient.finalizeAndCommitStream(client, parentTable);

		} catch (Exception e) {
			logger.error("Error processing collection {}.{}", dbName, collectionName, e);
		}
	}

	private void processRegularCollection(MongoCollection<Document> collection, String dbName,
			String collectionName, BigQueryWriteClient client) {
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
				bigQueryClient.convertAndSendBatch(batch, converter);
                bigQueryClient.rotateStreamIfNeeded(client);
			} catch (IOException e) {
				logger.error("Error sending batch", e);
			}

			// If we got fewer documents than config.getBatchSize(), we're done
			if (count < config.getBatchSize()) {
				break;
			}
		}
	}

	private void processTimeSeriesCollection(MongoCollection<Document> collection, Document collectionInfo,
			String streamName, String dbName, String collectionName, String tableName, BigQueryWriteClient client) {
		
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
				bigQueryClient.convertAndSendBatch(batch, converter);
                
                // Check if we need to rotate the stream
				bigQueryClient.rotateStreamIfNeeded(client);
                
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