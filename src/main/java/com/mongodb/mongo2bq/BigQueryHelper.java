package com.mongodb.mongo2bq;

import java.util.ArrayList;
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
    
    // Number of documents to sample for schema inference
    private static final int SCHEMA_SAMPLE_SIZE = 10;
    
    public static boolean tableExists(BigQuery bigQuery, TableId tableId) {
        return bigQuery.getTable(tableId) != null;
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