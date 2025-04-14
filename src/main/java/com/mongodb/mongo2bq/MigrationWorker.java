package com.mongodb.mongo2bq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.BidiStream;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.ByteString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MigrationWorker implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(MigrationWorker.class);
	
	private MongoToBigQueryConfig config;
	private MongoClient mongoClient;
	private Namespace ns;
	private Document collectionInfo;
	
	
	public MigrationWorker(MongoToBigQueryConfig config, String mongoClientName, Namespace ns, Document collectionInfo) {
		this.config = config;
		this.mongoClient = config.getMongoClient(mongoClientName);
		this.ns = ns;
		this.collectionInfo = collectionInfo;
	}

	@Override
	public void run() {
		
		TableId tableId = TableId.of(config.getBqDatasetName(), ns.getDatabaseName() + "_" + ns.getCollectionName());
		MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());
		
		if (!BigQueryHelper.tableExists(config.getBigQuery(), tableId)) {
			BigQueryHelper.createBigQueryTable(config.getBigQuery(), db.getCollection(ns.getCollectionName()), tableId);
		} else {
			logger.debug("BigQuery table already exists", tableId);
		}
		processCollection(config.getBigQueryClient(), db.getCollection(ns.getCollectionName()), ns.getDatabaseName(), collectionInfo);
	}
	
	private void processCollection(BigQueryWriteClient client, MongoCollection<Document> collection, String dbName,
			Document collectionInfo) {
		try {
			String collectionType = collectionInfo.getString("type");
			String collectionName = collectionInfo.getString("name");
			String tableName = dbName + "_" + collectionName;
			String parentTable = TableName.of(config.getGcpProjectId(), config.getBqDatasetName(), tableName)
					.toString();

			WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();
			WriteStream writeStream = client.createWriteStream(
					CreateWriteStreamRequest.newBuilder().setParent(parentTable).setWriteStream(stream).build());
			String streamName = writeStream.getName();

			BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream = client.appendRowsCallable().call();
			logger.info("Processing MongoDB Collection: {}.{}", dbName, collectionName);

			if ("collection".equals(collectionType)) {
				processRegularCollection(collection, streamName, bidiStream, dbName, collectionName);
			} else if ("timeseries".equals(collectionType)) {
				processTimeSeriesCollection(collection, collectionInfo, streamName, bidiStream, dbName, collectionName);
			} else {
				logger.debug("Skipping collection type: {}, name: {}", collectionType, collectionName);
				return;
			}

			bidiStream.closeSend();
			
			FinalizeWriteStreamRequest finalizeRequest = FinalizeWriteStreamRequest.newBuilder().setName(streamName)
					.build();
			client.finalizeWriteStream(finalizeRequest);

			BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
					.setParent(parentTable).addWriteStreams(streamName).build();
			BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);

			List<StorageError> errors = commitResponse.getStreamErrorsList();
			for (StorageError e : errors) {
				logger.error(e.getErrorMessage());
			}

			if (commitResponse.hasCommitTime()) {
				logger.info("Data committed to BigQuery table {} at {}", tableName, commitResponse.getCommitTime());
			} else {
				logger.error("Commit failed for table {}", tableName);
			}

		} catch (Exception e) {
			logger.error("Error processing collection {}.{}", dbName, collectionInfo, e);
		}
	}

	private void processRegularCollection(MongoCollection<Document> collection, String streamName,
			BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream, String dbName, String collectionName) {
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
			logger.info("Sending batch of {} documents from {}.{} (regular collection)", batch.size(), dbName,
					collectionName);
			try {
				convertAndSendBatch(batch, streamName, bidiStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// If we got fewer documents than config.getBatchSize(), we're done
			if (count < config.getBatchSize()) {
				break;
			}
		}
	}

	private void convertAndSendBatch(List<Document> batch, String streamName,
			BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream) throws IOException {
		ByteString arrowData = convertToArrow(batch);
		AppendRowsRequest request = AppendRowsRequest.newBuilder().setWriteStream(streamName)
				.setProtoRows(AppendRowsRequest.ProtoData.newBuilder()
						.setRows(ProtoRows.newBuilder().addSerializedRows(arrowData).build()).build())
				.build();

		bidiStream.send(request);
	}

	private void processTimeSeriesCollection(MongoCollection<Document> collection, Document collectionInfo,
			String streamName, BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream, String dbName,
			String collectionName) {
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
				convertAndSendBatch(batch, streamName, bidiStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// If cursor has no more documents and no lookahead saved, we're done
			if (!cursor.hasNext() && savedLookaheadDoc == null) {
				break;
			}
		}
	}

	private static ByteString convertToArrow(List<Document> batch) throws IOException {
		try (RootAllocator allocator = new RootAllocator(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {

			Set<String> fieldNames = new HashSet<>();
			for (Document doc : batch) {
				fieldNames.addAll(doc.keySet());
			}

			Map<String, FieldVector> vectors = new HashMap<>();
			for (String fieldName : fieldNames) {
				vectors.put(fieldName, new org.apache.arrow.vector.VarCharVector(fieldName, allocator));
			}

			try (VectorSchemaRoot root = new VectorSchemaRoot(new ArrayList<>(vectors.values()));
					ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

				writer.start();
				for (Document doc : batch) {
					for (Map.Entry<String, FieldVector> entry : vectors.entrySet()) {
						String field = entry.getKey();
						FieldVector vector = entry.getValue();

						if (doc.containsKey(field)) {
							Object value = doc.get(field);
							if (value instanceof String) {
								((org.apache.arrow.vector.VarCharVector) vector).setSafe(0,
										value.toString().getBytes());
							}
						}
					}
				}
				writer.end();
			}

			return ByteString.copyFrom(out.toByteArray());
		}
	}

}
