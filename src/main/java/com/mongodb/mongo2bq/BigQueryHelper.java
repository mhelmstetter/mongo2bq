package com.mongodb.mongo2bq;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class BigQueryHelper {
	
	private static final Logger logger = LoggerFactory.getLogger(BigQueryHelper.class);
	
	public static boolean tableExists(BigQuery bigQuery, TableId tableId) {
		return bigQuery.getTable(tableId) != null;
	}
    
	public static void createBigQueryTable(BigQuery bigQuery, MongoCollection<Document> collection, TableId tableId) {
		Map<String, Field> uniqueFields = new LinkedHashMap<>(); // Preserves insertion order and ensures uniqueness

		FindIterable<Document> sampleDocs = collection.find().limit(5);
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
	
	private static StandardSQLTypeName inferBigQueryType(Object value) {
		if (value instanceof Integer || value instanceof Long) {
			return StandardSQLTypeName.INT64;
		} else if (value instanceof Double || value instanceof Float) {
			return StandardSQLTypeName.FLOAT64;
		} else if (value instanceof Boolean) {
			return StandardSQLTypeName.BOOL;
		} else if (value instanceof Date || value instanceof java.sql.Timestamp) {
			return StandardSQLTypeName.TIMESTAMP;
		} else if (value instanceof List) {
			return StandardSQLTypeName.STRING; // Assuming list elements are strings
		} else {
			return StandardSQLTypeName.STRING;
		}
	}

}
