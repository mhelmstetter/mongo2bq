package com.mongodb.mongo2bq;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class BigQueryHelper {
    
    private static final Logger logger = LoggerFactory.getLogger(BigQueryHelper.class);
    
    // Number of documents to sample for schema inference
    private static final int SCHEMA_SAMPLE_SIZE = 10;
    
    public static boolean tableExists(BigQuery bigQuery, TableId tableId) {
        return bigQuery.getTable(tableId) != null;
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
                fieldNames.add(field.getName().toLowerCase());
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