package com.mongodb.mongo2bq;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest.ProtoData;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Helper class for converting MongoDB documents to Protobuf messages for BigQuery streaming.
 */
public class ProtoSchemaConverter {

    private static final Logger logger = LoggerFactory.getLogger(ProtoSchemaConverter.class);
    private static final Map<String, CachedProtoSchema> schemaCache = new ConcurrentHashMap<>();

    /**
     * Class to cache the generated Protobuf schema information
     */
    private static class CachedProtoSchema {
        final FileDescriptor fileDescriptor;
        final Descriptor messageDescriptor;
        final ProtoSchema protoSchema;

        CachedProtoSchema(FileDescriptor fileDescriptor, Descriptor messageDescriptor, ProtoSchema protoSchema) {
            this.fileDescriptor = fileDescriptor;
            this.messageDescriptor = messageDescriptor;
            this.protoSchema = protoSchema;
        }
    }

    /**
     * Generate a Protobuf schema from MongoDB document samples
     */
    public static ProtoSchema generateProtoSchema(List<Document> sampleDocs, String tableName) {
        // Check cache first
        String cacheKey = tableName;
        if (schemaCache.containsKey(cacheKey)) {
            return schemaCache.get(cacheKey).protoSchema;
        }

        // Map to store field names and their types
        Map<String, Type> fieldTypes = new HashMap<>();
        
        // Analyze fields from sample documents
        for (Document doc : sampleDocs) {
            for (String key : doc.keySet()) {
                Object value = doc.get(key);
                Type protoType = inferProtoType(value);
                
                // If field already exists, reconcile types
                if (fieldTypes.containsKey(key)) {
                    Type existingType = fieldTypes.get(key);
                    // If types don't match, use STRING as a fallback
                    if (existingType != protoType) {
                        fieldTypes.put(key, Type.TYPE_STRING);
                    }
                } else {
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
                FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder()
                        .setName(entry.getKey())
                        .setNumber(fieldNumber++)
                        .setType(entry.getValue());
                
                // Ensure field name is valid for protobuf
                String validFieldName = makeValidProtoFieldName(entry.getKey());
                fieldBuilder.setName(validFieldName);
                
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

            // Create ProtoSchema for BigQuery
            ProtoSchema protoSchema = ProtoSchema.newBuilder()
                    .setProtoDescriptor(fileDescriptorSet.getFile(0).toByteString())
                    .build();

            // Cache the result
            schemaCache.put(cacheKey, new CachedProtoSchema(fileDescriptor, messageDescriptor, protoSchema));
            
            return protoSchema;
        } catch (Exception e) {
            logger.error("Error generating Protobuf schema", e);
            throw new RuntimeException("Failed to generate Protobuf schema", e);
        }
    }

    /**
     * Make field names protobuf-compatible
     */
    private static String makeValidProtoFieldName(String name) {
        // Replace any invalid character with underscore
        String validName = name.replaceAll("[^a-zA-Z0-9_]", "_");
        
        // Ensure it starts with a letter or underscore
        if (!validName.isEmpty() && !Character.isLetter(validName.charAt(0)) && validName.charAt(0) != '_') {
            validName = "_" + validName;
        }
        
        return validName;
    }

    /**
     * Infer the appropriate Protobuf type for a value
     */
    private static Type inferProtoType(Object value) {
        if (value == null) {
            return Type.TYPE_STRING;  // Default to string for null values
        } else if (value instanceof Integer) {
            return Type.TYPE_INT32;
        } else if (value instanceof Long) {
            return Type.TYPE_INT64;
        } else if (value instanceof Double || value instanceof Float) {
            return Type.TYPE_DOUBLE;
        } else if (value instanceof Boolean) {
            return Type.TYPE_BOOL;
        } else if (value instanceof Date) {
            return Type.TYPE_INT64;  // Store timestamps as int64 (millis since epoch)
        } else if (value instanceof ObjectId) {
            return Type.TYPE_STRING;  // Convert ObjectId to string
        } else if (value instanceof List) {
            return Type.TYPE_STRING;  // Serialize lists as JSON strings
        } else if (value instanceof Document) {
            return Type.TYPE_STRING;  // Serialize subdocuments as JSON strings
        } else {
            return Type.TYPE_STRING;  // Default to string for any other type
        }
    }

    /**
     * Convert MongoDB documents to Protobuf rows
     */
    public static ProtoRows convertDocumentsToProtoRows(List<Document> documents, String tableName) {
        // Get cached schema
        CachedProtoSchema cachedSchema = schemaCache.get(tableName);
        if (cachedSchema == null) {
            throw new IllegalStateException("Schema not generated for " + tableName);
        }

        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        
        // Convert each document to a protobuf message
        for (Document doc : documents) {
            DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(cachedSchema.messageDescriptor);
            
            for (FieldDescriptor fieldDescriptor : cachedSchema.messageDescriptor.getFields()) {
                String fieldName = fieldDescriptor.getName();
                
                // Find the original MongoDB field name that maps to this protobuf field
                String originalFieldName = findOriginalFieldName(fieldName, doc.keySet());
                
                if (originalFieldName != null && doc.containsKey(originalFieldName)) {
                    Object value = doc.get(originalFieldName);
                    setFieldValue(messageBuilder, fieldDescriptor, value);
                }
            }
            
            DynamicMessage message = messageBuilder.build();
            protoRowsBuilder.addSerializedRows(message.toByteString());
        }
        
        return protoRowsBuilder.build();
    }
    
    /**
     * Find the original field name in MongoDB document that corresponds to the protobuf field name
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
     * Set a field value in the Protobuf message builder
     */
    private static void setFieldValue(DynamicMessage.Builder builder, FieldDescriptor field, Object value) {
        if (value == null) {
            return; // Skip null values
        }
        
        try {
            switch (field.getType()) {
                case INT32:
                    if (value instanceof Number) {
                        builder.setField(field, ((Number) value).intValue());
                    }
                    break;
                case INT64:
                    if (value instanceof Number) {
                        builder.setField(field, ((Number) value).longValue());
                    } else if (value instanceof Date) {
                        builder.setField(field, ((Date) value).getTime());
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Number) {
                        builder.setField(field, ((Number) value).doubleValue());
                    }
                    break;
                case BOOL:
                    if (value instanceof Boolean) {
                        builder.setField(field, value);
                    }
                    break;
                case STRING:
                    builder.setField(field, value.toString());
                    break;
                default:
                    // For other types, convert to string
                    builder.setField(field, value.toString());
            }
        } catch (Exception e) {
            logger.warn("Could not set field '{}' with value '{}': {}", field.getName(), value, e.getMessage());
        }
    }

    /**
     * Create an AppendRowsRequest with the appropriate schema and rows
     */
    public static AppendRowsRequest createAppendRequest(String streamName, List<Document> batch, String tableName, boolean includeSchema) {
        ProtoSchema protoSchema = null;
        
        // Generate schema if needed
        if (!schemaCache.containsKey(tableName)) {
            protoSchema = generateProtoSchema(batch, tableName);
        } else if (includeSchema) {
            protoSchema = schemaCache.get(tableName).protoSchema;
        }
        
        // Convert documents to proto rows
        ProtoRows rows = convertDocumentsToProtoRows(batch, tableName);
        
        // Build the request
        ProtoData.Builder dataBuilder = ProtoData.newBuilder().setRows(rows);
        if (protoSchema != null) {
            dataBuilder.setWriterSchema(protoSchema);
        }
        
        return AppendRowsRequest.newBuilder()
                .setWriteStream(streamName)
                .setProtoRows(dataBuilder.build())
                .build();
    }
}