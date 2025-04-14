package com.mongodb.mongo2bq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.BidiStream;
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

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "mongo2bq", mixinStandardHelpOptions = true, version = "mongo2bq 0.1", description = "MongoDB to BigQuery sync tool")
public class MongoToBigQuery implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(MongoToBigQuery.class);

	@Option(names = { "--config" }, required = false, defaultValue = "mongo2bq.properties")
	private String configFile;

	private MongoToBigQueryConfig config;
	
	private ExecutorService executor;
	
	private List<MigrationWorker> workers = new ArrayList<>();

	public void run() {
		Runtime.Version version = Runtime.version();
		logger.debug("Java runtime version {}", version);

		try {
			config = new MongoToBigQueryConfig(configFile);
		} catch (org.apache.commons.configuration2.ex.ConfigurationException | IOException e) {
			logger.error("Error processing config file {}, error: {}", configFile, e.getMessage());
			return;
		}
		
		if (config.getIncludeNamespaces().isEmpty()) {
			logger.error("includeNamespaces not found in config");
			return;
		}

		try {
			
			
			for (Map.Entry<String, MongoClient> entry : config.getMongoClients().entrySet()) {
				
				for (Namespace ns : config.getIncludeNamespaces()) {
					Document result = MongoHelper.getCollectionInfo(entry.getValue(), ns);
					MigrationWorker worker = new MigrationWorker(config, entry.getKey(), ns, result);
					workers.add(worker);
				}
			}
			
			executor = Executors.newFixedThreadPool(workers.size());
			
			for (MigrationWorker worker : workers) {
				executor.execute(worker);
			}

	        executor.shutdown();
	        while (!executor.isTerminated()) {
	            Thread.sleep(1000);
	        }
	        logger.debug("MongoToBigQuery complete");

//			for (String dbName : mongoClient.listDatabaseNames()) {
//				if (List.of("admin", "config", "local").contains(dbName))
//					continue;
//
//				MongoDatabase db = mongoClient.getDatabase(dbName);
//				for (Document collectionInfo : db.listCollections()) {
//					String collectionName = collectionInfo.getString("name");
//					TableId tableId = TableId.of(config.getBqDatasetName(), dbName + "_" + collectionName);
//
//					if (!tableExists(bigQuery, tableId)) {
//						createBigQueryTable(bigQuery, db.getCollection(collectionName), tableId);
//					} else {
//						logger.debug("BigQuery table already exists", tableId);
//					}
//					processCollection(bigQueryClient, db.getCollection(collectionName), dbName, collectionInfo);
//				}
//			}
		} catch (Exception e) {
			logger.error("Error processing MongoDB to BigQuery", e);
		}
	}

	public static void main(String[] args) {
		MongoToBigQuery m2bq = new MongoToBigQuery();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				System.out.println();
				System.out.println("**** SHUTDOWN *****");
				m2bq.stop();
			}
		}));

		int exitCode = new CommandLine(m2bq).execute(args);
		System.exit(exitCode);
	}

	protected void stop() {
		// TODO Auto-generated method stub

	}

}
