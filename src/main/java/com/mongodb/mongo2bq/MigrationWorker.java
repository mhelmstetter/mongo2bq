package com.mongodb.mongo2bq;

import static com.mongodb.client.model.Filters.eq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MigrationWorker implements Runnable {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MigrationWorker.class);

	private MongoToBigQueryConfig config;
	private MongoClient mongoClient;
	
	private MongoClient metaMongoClient;
	private MongoDatabase metaMongoDb;
	private MongoCollection<Document> metaCollection;
	
	private Namespace ns;
	private Document collectionInfo;
	private ProtoSchemaConverter converter;
	private boolean stopRequested = false;
	
	private String syncStateId;
	
	private BigQueryClient bigQueryClient;
    
	public MigrationWorker(MongoToBigQueryConfig config, String mongoClientName, Namespace ns,
			Document collectionInfo) {
		this.config = config;
		this.syncStateId = mongoClientName + "_" + ns.getDatabaseName() + "_" + ns.getCollectionName();
		this.mongoClient = config.getMongoClient(mongoClientName);
		this.metaMongoClient = config.getMetaMongoClient();
		this.metaMongoDb = metaMongoClient.getDatabase("mongo2bqMeta");
		this.metaCollection = metaMongoDb.getCollection("syncState");
		
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
	
	private Document getSyncStateDocument() {
		Document syncState = metaCollection.find(eq("_id", syncStateId)).first();
		if (syncState == null) {
			syncState = new Document("_id", syncStateId);
		}
		return syncState;
	}
	
	private void saveSyncStateDocument(Document syncState) {
		
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

			logger.info("Processing MongoDB Collection: {}.{}, type: {}", dbName, collectionName, collectionType);

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

			// If we got no documents, loop again
			if (count == 0) {
				continue;
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
			String streamName, String dbName, String collectionName, String tableName, BigQueryWriteClient client) throws Exception {
		
		// Extract timeField from collection info
		Document options = (Document) collectionInfo.get("options");
		Document timeseries = (Document) options.get("timeseries");
		String timeField = timeseries.getString("timeField");

		// Sort by timeField
		Document sort = new Document(timeField, 1);
		Date lastTimeValue = null;
		boolean isFirstBatch = true;
		Document savedLookaheadDoc = null;
		
		Document syncStateDocument = getSyncStateDocument();
		lastTimeValue = syncStateDocument.getDate("lastSyncTime");

		while (true) {
			// Build query - for subsequent batches, filter by timeField > lastTimeValue
			FindIterable<Document> docs;
			if (isFirstBatch && lastTimeValue.equals(null)) {
				docs = collection.find().sort(sort);
				isFirstBatch = false;
			} else {
				if (lastTimeValue.equals(null)) {
					throw new Exception("lastTimeValue not populated");
				}
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

			Date currentTimeValue = null;
			boolean batchComplete = false;
			int count = batch.size(); // Start with the count of any previously saved documents

			while (cursor.hasNext() && !batchComplete) {
				Document doc = cursor.next();
				currentTimeValue = doc.getDate(timeField);

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
				continue;
			}

			// Send batch to BigQuery
			logger.info(
					"Sending batch of {} documents from {}.{} (timeseries collection) with timeField range: {} to {}",
					batch.size(), dbName, collectionName, batch.get(0).get(timeField),
					batch.get(batch.size() - 1).get(timeField));
			try {
				bigQueryClient.convertAndSendBatch(batch, converter);
				bigQueryClient.rotateStreamIfNeeded(client);
			} catch (IOException e) {
				logger.error("Error sending batch", e);
			}
		}
	}
}