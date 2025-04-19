package com.mongodb.mongo2bq;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoToBigQueryConfig {
	
	protected static Logger logger = LoggerFactory.getLogger(BaseConfiguration.class);
	

    private final static String INCLUDE_NAMESPACES = "includeNamespaces";
    
    private static final String PROJECT_ID = "gcpProjectId";
	private static final String DATASET_NAME = "bqDatasetName";
	private static final String METADATA_MONGO_URI = "metadataMongoUri";
	private static final String SOURCE_MONGO_URIS = "sourceMongoUris";
	private static final String SOURCE_MONGO_NAMES = "sourceMongoNames";
	private static final String BATCH_SIZE = "batchSize";
	private static final String SERVICE_ACCOUNT_KEY_PATH = "serviceAccountKeyPath";
	
	private static final String MAX_ROWS_PER_STREAM = "maxRowsPerStream";
	private static final String MAX_STREAM_DURATION_MINUTES = "maxStreamDurationMinutes";
	private static final String MAX_MEGABYTES_PER_STREAM = "maxMegabytesPerStream";
	
    // Constants that control when to finalize streams
    private static final long DEFAULT_ROWS_PER_STREAM = 100_000; // 1 million rows
    private static final long DEFAULT_STREAM_DURATION_MINUTES = 30; // 30 minutes
    private static final long DEFAULT_MEGABYTES_PER_STREAM = 4096; // 4 GB
    
    protected Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	protected Set<String> includedNamespaceStrings = new HashSet<String>();
	
	
	protected Set<String> includeDatabases = new HashSet<String>();
	protected Set<String> includeDatabasesAll = new HashSet<String>();
	private boolean filtered;
	
	private String gcpProjectId;
	private String bqDatasetName;
	private String[] sourceMongoUris;
	private String[] sourceMongoNames;
	private String metaMongoUri;
    private int batchSize;
    
    private long maxRowsPerStream;
    private long maxStreamDurationMinutes;
    private long maxMegabytesPerStream;
    
    private Map<String, MongoClient> mongoClients = new LinkedHashMap<>();
    private MongoClient metaMongoClient;
    
    private String serviceAccountKeyPath;
    private BigQueryWriteClient bigQueryClient;
    private BigQuery bigQuery;
	
    
    public MongoToBigQueryConfig(String configFile) throws org.apache.commons.configuration2.ex.ConfigurationException, IOException {
    	FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<>(
				PropertiesConfiguration.class)
				.configure(new Parameters().properties().setFileName(configFile).setThrowExceptionOnMissing(true)
						.setListDelimiterHandler(new DefaultListDelimiterHandler(',')).setIncludesAllowed(false));
		PropertiesConfiguration config = null;
		config = builder.getConfiguration();
		
		String[] includes = config.getStringArray(INCLUDE_NAMESPACES);
		setNamespaceFilters(includes);
		sourceMongoNames = config.getStringArray(SOURCE_MONGO_NAMES);
		
		gcpProjectId = config.getString(PROJECT_ID);
		bqDatasetName = config.getString(DATASET_NAME);
		sourceMongoUris = config.getStringArray(SOURCE_MONGO_URIS);
		metaMongoUri = config.getString(METADATA_MONGO_URI);
		batchSize = config.getInt(BATCH_SIZE, 10000);
		serviceAccountKeyPath = config.getString(SERVICE_ACCOUNT_KEY_PATH, null);
		
		maxRowsPerStream = config.getLong(MAX_ROWS_PER_STREAM, DEFAULT_ROWS_PER_STREAM);
		maxStreamDurationMinutes = config.getLong(MAX_STREAM_DURATION_MINUTES, DEFAULT_STREAM_DURATION_MINUTES);
		maxMegabytesPerStream = config.getLong(MAX_MEGABYTES_PER_STREAM, DEFAULT_MEGABYTES_PER_STREAM);
		
		if (sourceMongoUris.length == 0) {
			throw new IllegalArgumentException(SOURCE_MONGO_URIS + " not defined in properties");
		}
		if (sourceMongoNames.length != sourceMongoUris.length) {
			throw new IllegalArgumentException(SOURCE_MONGO_NAMES + " length does not match " + SOURCE_MONGO_URIS + " length");
		}
		
		int i = 0;
		for (String uri : sourceMongoUris) {
			MongoClient mongoClient = MongoClients.create(uri);
			String name = sourceMongoNames[i++];
			mongoClients.put(name, mongoClient);
		}
		metaMongoClient = MongoClients.create(metaMongoUri);
		
		bigQueryClient = BigQueryClient.createBigQueryClient(this);

		bigQuery = BigQueryOptions.getDefaultInstance().getService();
		ensureDatasetExists();
    }
	
	private void ensureDatasetExists() {
		Dataset dataset = bigQuery.getDataset(bqDatasetName);
		if (dataset == null) {
			DatasetInfo datasetInfo = DatasetInfo.newBuilder(bqDatasetName).build();
			bigQuery.create(datasetInfo);
			logger.info("Created dataset: {}", bqDatasetName);
		}
	}
    
    public MongoClient getMongoClient(String clientName) {
    	MongoClient mc = mongoClients.get(clientName);
    	if (mc == null) {
    		throw new IllegalArgumentException("MongoClient with sourceMongoName " + clientName + " not found");
    	}
    	return mc;
    }
    
	public boolean filterCheck(String nsStr) {
		Namespace ns = new Namespace(nsStr);
		return filterCheck(ns);
	}
	
	public boolean filterCheck(Namespace ns) {
		if (isFiltered() && !includeNamespaces.contains(ns) && !includeDatabases.contains(ns.getDatabaseName())) {
			logger.trace("Namespace " + ns + " filtered, skipping");
			return true;
		}
		if (ns.getDatabaseName().equals("config") || ns.getDatabaseName().equals("admin") || ns.getDatabaseName().equals("local")) {
			return true;
		}
		if (ns.getCollectionName().equals("system.profile") || ns.getCollectionName().equals("system.users")) {
			return true;
		}
		return false;
	}
	
	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null || namespaceFilterList.length == 0) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				includedNamespaceStrings.add(nsStr);
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
				includeDatabasesAll.add(ns.getDatabaseName());
			} else {
				includeDatabases.add(nsStr);
				includeDatabasesAll.add(nsStr);
			}
		}
	}

	public boolean isFiltered() {
		return filtered;
	}

	public void setFiltered(boolean filtered) {
		this.filtered = filtered;
	}

	public Set<Namespace> getIncludeNamespaces() {
		return includeNamespaces;
	}
	
	public Set<String> getIncludedNamespaceStrings() {
		return includedNamespaceStrings;
	}

	public void setIncludeNamespaces(Set<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}

	public Set<String> getIncludeDatabases() {
		return includeDatabases;
	}

	public void setIncludeDatabases(Set<String> includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

	public Set<String> getIncludeDatabasesAll() {
		return includeDatabasesAll;
	}

	public void setIncludeDatabasesAll(Set<String> includeDatabasesAll) {
		this.includeDatabasesAll = includeDatabasesAll;
	}

	public String getGcpProjectId() {
		return gcpProjectId;
	}

	public void setGcpProjectId(String gcpProjectId) {
		this.gcpProjectId = gcpProjectId;
	}

	public String getBqDatasetName() {
		return bqDatasetName;
	}

	public void setBqDatasetName(String bqDatasetName) {
		this.bqDatasetName = bqDatasetName;
	}

	public String[] getSourceMongoUris() {
		return sourceMongoUris;
	}

	public void setSourceMongoUris(String[] souceMongoUri) {
		this.sourceMongoUris = souceMongoUri;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String[] getSourceMongoNames() {
		return sourceMongoNames;
	}

	public void setSourceMongoNames(String[] sourceMongoNames) {
		this.sourceMongoNames = sourceMongoNames;
	}

	public Map<String, MongoClient> getMongoClients() {
		return mongoClients;
	}

	public BigQueryWriteClient getBigQueryClient() {
		return bigQueryClient;
	}

	public BigQuery getBigQuery() {
		return bigQuery;
	}

	public long getMaxRowsPerStream() {
		return maxRowsPerStream;
	}

	public long getMaxStreamDurationMinutes() {
		return maxStreamDurationMinutes;
	}

	public long getMaxMegabytesPerStream() {
		return maxMegabytesPerStream;
	}

	public String getServiceAccountKeyPath() {
		return serviceAccountKeyPath;
	}

	public MongoClient getMetaMongoClient() {
		return metaMongoClient;
	}
    
    
//	protected void parseArgs() throws ConfigurationException {
//
//		Configuration config = readProperties();
//		
//		balancerConfig.setSourceClusterUri(config.getString(SOURCE_URI));
//		String[] includes = config.getStringArray(INCLUDE_NAMESPACES);
//		balancerConfig.setNamespaceFilters(includes);
//
//		String[] sourceShards = config.getStringArray(SOURCE_SHARDS);
//		balancerConfig.setSourceShards(sourceShards);
//		balancerConfig.setAnalyzerSleepIntervalMinutes(config.getInt(ANALYZER_SLEEP_INTERVAL, 15));
//		balancerConfig.setDryRun(config.getBoolean(DRY_RUN, false));
//		balancerConfig.setDeltaThresholdPercent(config.getDouble(DELTA_THRESHOLD_PERCENT, 3.0));
//		balancerConfig.setMoveCountBackoffThreshold(config.getInt(MOVE_COUNT_BACKOFF_THRESHOLD, 10));
//		balancerConfig.setActiveChunkThreshold(config.getInt(ACTIVE_CHUNK_THRESHOLD, 10));
//	}



}
