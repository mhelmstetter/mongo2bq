package com.mongodb.mongo2bq;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.types.ObjectId;
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
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest.ProtoData;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

/**
 * Helper class for converting MongoDB documents to Protobuf messages for
 * BigQuery streaming.
 */
public class ProtoSchemaConverter {

	private static final Logger logger = LoggerFactory.getLogger(ProtoSchemaConverter.class);

	private static final Map<String, CachedProtoSchema> schemaCache = new ConcurrentHashMap<>();
	private static final Map<String, Set<String>> seenFieldsPerTable = new ConcurrentHashMap<>();
	private static final Map<String, Set<String>> seenFieldsPerCollection = new ConcurrentHashMap<>();
	
	private MongoToBigQueryConfig config;

	
	public ProtoSchemaConverter(MongoToBigQueryConfig config) {
		this.config = config;
	}

	/**
	 * Generate a Protobuf schema using BigQuery table's schema as a guide
	 */
	public ProtoSchema generateProtoSchema(List<Document> sampleDocs, String tableName) {
	    // Check cache first
	    String cacheKey = tableName;
	    if (schemaCache.containsKey(cacheKey)) {
	        return schemaCache.get(cacheKey).protoSchema;
	    }

	    // Get BigQuery schema for type matching
	    Map<String, LegacySQLTypeName> bigQueryFields = BigQueryClient.getBigQueryFieldTypes(config.getGcpProjectId(), config.getBqDatasetName(), tableName);
	    
	    // Map to store field names and their types
	    Map<String, Type> fieldTypes = new HashMap<>();
	    
	    // Analyze fields from sample documents
	    for (Document doc : sampleDocs) {
	        for (String key : doc.keySet()) {
	            // IMPORTANT: Don't convert to lowercase here - preserve original case
	            String validFieldName = makeValidProtoFieldName(key);
	            
	            // If field exists in BigQuery, use its type
	            if (bigQueryFields != null) {
	                // Use case-insensitive lookup for type
	                LegacySQLTypeName bqType = null;
	                for (Map.Entry<String, LegacySQLTypeName> entry : bigQueryFields.entrySet()) {
	                    if (entry.getKey().equalsIgnoreCase(validFieldName)) {
	                        bqType = entry.getValue();
	                        break;
	                    }
	                }
	                
	                if (bqType != null) {
	                    Type protoType = BigQueryClient.convertBigQueryTypeToProtoType(bqType);
	                    // Use the original case for the field name
	                    fieldTypes.put(key, protoType);
	                } else {
	                    // For new fields, infer type from MongoDB value
	                    Object value = doc.get(key);
	                    Type protoType = inferProtoType(value);
	                    fieldTypes.put(key, protoType);
	                }
	            } else {
	                // For new fields, infer type from MongoDB value
	                Object value = doc.get(key);
	                Type protoType = inferProtoType(value);
	                fieldTypes.put(key, protoType);
	            }
	        }
	    }

	    try {
	        // Create descriptors
	        FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
	        FileDescriptorProto.Builder fileDescriptorProtoBuilder = FileDescriptorProto.newBuilder()
	                .setName(tableName + ".proto")
	                .setPackage("dynamicproto");

	        // Build message descriptor
	        DescriptorProto.Builder messageBuilder = DescriptorProto.newBuilder()
	                .setName("DynamicMessage");

	        int fieldNumber = 1;
	        for (Map.Entry<String, Type> entry : fieldTypes.entrySet()) {
	            // CRITICAL: Use original key with proper casing
	            String originalKey = entry.getKey();
	            String validFieldName = makeValidProtoFieldName(originalKey);
	            
	            FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder()
	                    .setName(validFieldName)  // Use the properly cased field name
	                    .setNumber(fieldNumber++)
	                    .setType(entry.getValue());
	            
	            messageBuilder.addField(fieldBuilder.build());
	        }

	        fileDescriptorProtoBuilder.addMessageType(messageBuilder.build());
	        fileDescriptorSetBuilder.addFile(fileDescriptorProtoBuilder.build());
	        FileDescriptorSet fileDescriptorSet = fileDescriptorSetBuilder.build();

	        // Create file descriptor
	        FileDescriptor[] empty = new FileDescriptor[0];
	        FileDescriptor fileDescriptor = FileDescriptor.buildFrom(
	                fileDescriptorSet.getFile(0),
	                empty
	        );

	        // Get message descriptor
	        Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("DynamicMessage");

	        CachedProtoSchema cachedProtoSchema = new CachedProtoSchema(messageDescriptor);
	        // Cache the result
	        schemaCache.put(cacheKey, cachedProtoSchema);
	        
	        return cachedProtoSchema.getProtoSchema();
	    } catch (Exception e) {
	        logger.error("Error generating Protobuf schema", e);
	        throw new RuntimeException("Failed to generate Protobuf schema", e);
	    }
	}


	/**
	 * Make field names protobuf-compatible
	 */
	public static String makeValidProtoFieldName(String name) {
	    // Replace any invalid character with underscore
	    String validName = name.replaceAll("[^a-zA-Z0-9_]", "_");

	    // Ensure it starts with a letter or underscore
	    if (!validName.isEmpty() && !Character.isLetter(validName.charAt(0)) && validName.charAt(0) != '_') {
	        validName = "_" + validName;
	    }

	    if (!validName.equals(name)) {
	        logger.debug("Field name {} is not a valid protobuf field name, had to remap to {}", name, validName);
	    }

	    return validName;
	}

	/**
	 * Infer the appropriate Protobuf type for a value
	 */
	private static Type inferProtoType(Object value) {
		if (value == null) {
			return Type.TYPE_STRING; // Default to string for null values
		} else if (value instanceof Integer) {
			return Type.TYPE_INT32;
		} else if (value instanceof Long) {
			return Type.TYPE_INT64;
		} else if (value instanceof Double || value instanceof Float) {
			return Type.TYPE_DOUBLE;
		} else if (value instanceof Boolean) {
			return Type.TYPE_BOOL;
		} else if (value instanceof Date) {
			return Type.TYPE_INT64; // Store timestamps as int64 (millis since epoch)
		} else if (value instanceof ObjectId) {
			return Type.TYPE_STRING; // Convert ObjectId to string
		} else if (value instanceof List) {
			return Type.TYPE_STRING; // Serialize lists as JSON strings
		} else if (value instanceof Document) {
			return Type.TYPE_STRING; // Serialize subdocuments as JSON strings
		} else {
			return Type.TYPE_STRING; // Default to string for any other type
		}
	}

	/**
	 * Convert MongoDB documents to Protobuf rows
	 */
	public ProtoRows convertDocumentsToProtoRows(List<Document> documents, String tableName) {
		// Get cached schema
		CachedProtoSchema cachedSchema = schemaCache.get(tableName);
		if (cachedSchema == null) {
			throw new IllegalStateException("Schema not generated for " + tableName);
		}

		Set<String> knownBQFields = BigQueryClient.getBigQueryFields(config.getGcpProjectId(), config.getBqDatasetName(), tableName);
		Set<String> newFieldsThisBatch = new HashSet<>();

		// First pass: identify new fields that need to be added to BigQuery
		for (Document doc : documents) {
		    for (String key : doc.keySet()) {
		        String validFieldName = makeValidProtoFieldName(key);
		        // Case-insensitive comparison for existence check
		        boolean fieldExists = false;
		        if (knownBQFields != null) {
		            for (String knownField : knownBQFields) {
		                if (knownField.equalsIgnoreCase(validFieldName)) {
		                    fieldExists = true;
		                    break;
		                }
		            }
		        }
		        if (!fieldExists) {
		            newFieldsThisBatch.add(key);
		        }
		    }
		}

		// If we have new fields, update the BigQuery schema
		if (!newFieldsThisBatch.isEmpty()) {
			logger.info("Found new fields to add to BigQuery schema: {}", newFieldsThisBatch);
			boolean success = updateBigQuerySchema(config.getGcpProjectId(), config.getBqDatasetName(), tableName, newFieldsThisBatch, documents);
			if (!success) {
				logger.error("Failed to update BigQuery schema with new fields: {}", newFieldsThisBatch);
				// You may want to decide how to handle this - continue with existing fields or abort
			}
		}

		// Now continue with normal processing
		ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
		int successfulConversions = 0;

		for (Document doc : documents) {
			DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(cachedSchema.messageDescriptor);
			int fieldsSet = 0;

			for (FieldDescriptor fieldDescriptor : cachedSchema.messageDescriptor.getFields()) {
				String fieldName = fieldDescriptor.getName();
				String originalFieldName = findOriginalFieldName(fieldName, doc.keySet());

				if (originalFieldName != null && doc.containsKey(originalFieldName)) {
					Object value = doc.get(originalFieldName);
					boolean success = setFieldValue(messageBuilder, fieldDescriptor, value);
					if (success) {
						fieldsSet++;
					}
				}
			}

			if (fieldsSet > 0) {
				DynamicMessage message = messageBuilder.build();
				protoRowsBuilder.addSerializedRows(message.toByteString());
				successfulConversions++;
			} else {
				logger.warn("Document converted with zero fields set: {}", doc.get("_id"));
			}
		}

		logger.info("Converted {}/{} documents to proto rows for table {}", successfulConversions, documents.size(),
				tableName);

		return protoRowsBuilder.build();
	}

	/**
	 * Find the original field name in MongoDB document that corresponds to the
	 * protobuf field name
	 */
	private static String findOriginalFieldName(String protoFieldName, Set<String> documentKeys) {
		// Direct match
		if (documentKeys.contains(protoFieldName)) {
			return protoFieldName;
		}

		// Try to match based on the transformation rules
		for (String key : documentKeys) {
			if (makeValidProtoFieldName(key).equals(protoFieldName)) {
				return key;
			}
		}

		return null;
	}

	/**
	 * Update the BigQuery table schema to add new fields
	 */
	private static boolean updateBigQuerySchema(String projectId, String datasetId, String tableId,
			Set<String> newFields, List<Document> sampleDocs) {
		try {
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			TableId tableIdObj = TableId.of(projectId, datasetId, tableId);
			Table table = bigquery.getTable(tableIdObj);

			if (table == null) {
				logger.error("Table not found: {}", tableIdObj);
				return false;
			}

			// Get existing schema
			Schema existingSchema = table.getDefinition().getSchema();
			List<Field> existingFields = existingSchema.getFields();

			// Build new schema with additional fields
			List<Field> updatedFields = new ArrayList<>(existingFields);

			for (String newField : newFields) {
				String validFieldName = makeValidProtoFieldName(newField);

				// Skip if field already exists (case insensitive)
				if (existingFields.stream().anyMatch(f -> f.getName().equalsIgnoreCase(validFieldName))) {
					continue;
				}

				// Infer the field type from sample documents
				StandardSQLTypeName fieldType = BigQueryClient.inferBigQueryFieldType(newField, sampleDocs);
				Field field = Field.newBuilder(validFieldName, fieldType).setMode(Field.Mode.NULLABLE)
						.setDescription("Added automatically from MongoDB").build();

				updatedFields.add(field);
				logger.info("Adding new field to BigQuery schema: {}, type: {}", validFieldName, fieldType);
			}

			Schema updatedSchema = Schema.of(updatedFields);
			TableDefinition tableDefinition = StandardTableDefinition.of(updatedSchema);

			Table updatedTable = table.toBuilder().setDefinition(tableDefinition).build();

			// Update the table
			updatedTable = bigquery.update(updatedTable);
			logger.info("Successfully updated BigQuery schema for table: {}", tableIdObj);

			return true;
		} catch (Exception e) {
			logger.error("Error updating BigQuery schema: {}", e.getMessage(), e);
			return false;
		}
	}

	private static void updateSchemaWithNewFields(Set<String> newFields, CachedProtoSchema cachedSchema,
			String tableName) {
		DescriptorProto existingDescriptorProto = cachedSchema.messageDescriptor.toProto();
		DescriptorProto.Builder updatedDescriptorBuilder = existingDescriptorProto.toBuilder();

		int maxFieldNumber = cachedSchema.messageDescriptor.getFields().stream().mapToInt(FieldDescriptor::getNumber)
				.max().orElse(0);

		for (String fieldName : newFields) {
			FieldDescriptorProto.Type fieldType = FieldDescriptorProto.Type.TYPE_STRING; // Default guess
			FieldDescriptorProto.Builder newField = FieldDescriptorProto.newBuilder().setName(fieldName)
					.setType(fieldType).setNumber(++maxFieldNumber);

			updatedDescriptorBuilder.addField(newField);
		}

		FileDescriptorProto fileDescriptorProto = FileDescriptorProto.newBuilder()
				.addMessageType(updatedDescriptorBuilder).setName(tableName + ".proto").setSyntax("proto3").build();

		try {
			FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[] {});
			Descriptor updatedDescriptor = fileDescriptor.findMessageTypeByName(existingDescriptorProto.getName());
			CachedProtoSchema newSchema = new CachedProtoSchema(updatedDescriptor);
			schemaCache.put(tableName, newSchema);
			logger.info("Updated schema for table {} with new fields: {}", tableName, newFields);
		} catch (DescriptorValidationException e) {
			logger.error("Failed to update schema for {}: {}", tableName, e.getMessage(), e);
		}
	}

	/**
	 * Set a field value in the Protobuf message builder
	 */
	private static boolean setFieldValue(DynamicMessage.Builder builder, FieldDescriptor field, Object value) {
	    if (value == null) {
	        return false; // Skip null values
	    }

	    try {
	        switch (field.getType()) {
	        case INT32:
	            if (value instanceof Number) {
	                builder.setField(field, ((Number) value).intValue());
	                return true;
	            }
	            break;
	        case INT64:
	            if (value instanceof Number) {
	                builder.setField(field, ((Number) value).longValue());
	                return true;
	            } else if (value instanceof Date) {
	                // Convert Date to microseconds for BigQuery TIMESTAMP
	                // BigQuery expects timestamps in microseconds precision
	                long microseconds = ((Date) value).getTime() * 1000; // Convert millis to micros
	                builder.setField(field, microseconds);
	                return true;
	            }
	            break;
	        case DOUBLE:
	            if (value instanceof Number) {
	                builder.setField(field, ((Number) value).doubleValue());
	                return true;
	            }
	            break;
	        case BOOL:
	            if (value instanceof Boolean) {
	                builder.setField(field, value);
	                return true;
	            }
	            break;
	        case STRING:
	            builder.setField(field, value.toString());
	            return true;
	        default:
	            // For other types, convert to string
	            builder.setField(field, value.toString());
	            return true;
	        }
	    } catch (Exception e) {
	        logger.warn("Could not set field '{}' with value '{}': {}", field.getName(), value, e.getMessage());
	    }
	    return false;
	}

	/**
	 * Create an AppendRowsRequest with the appropriate schema and rows
	 */
	public AppendRowsRequest createAppendRequest(String streamName, List<Document> batch, String tableName, Set<String> allowedFields) {
	    
	    // Always generate schema
	    ProtoSchema protoSchema = null;
	    
	    // Generate schema if needed or not in cache
	    if (!schemaCache.containsKey(tableName)) {
	        logger.info("Generating new schema for table {}", tableName);
	        protoSchema = generateProtoSchema(batch, tableName);
	    } else {
	        logger.info("Using cached schema for table {}", tableName);
	        protoSchema = schemaCache.get(tableName).protoSchema;
	    }

	    // Convert documents to proto rows
	    ProtoRows rows = convertDocumentsToProtoRows(batch, tableName);

	    // Validate rows
	    if (rows.getSerializedRowsCount() == 0) {
	        logger.warn("No rows were converted from batch of {} documents!", batch.size());
	        return null; // Return null to indicate no data to send
	    }

	    // Build the request - ALWAYS include the schema
	    ProtoData.Builder dataBuilder = ProtoData.newBuilder().setRows(rows);
	    
	    // Always include the schema
	    dataBuilder.setWriterSchema(protoSchema);
	    logger.debug("Including schema in append request for stream {}", streamName);

	    logger.debug("Created append request with {} rows for stream {}", rows.getSerializedRowsCount(), streamName);

	    return AppendRowsRequest.newBuilder().setWriteStream(streamName).setProtoRows(dataBuilder.build()).build();
	}
}