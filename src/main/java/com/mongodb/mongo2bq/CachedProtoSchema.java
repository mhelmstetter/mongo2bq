package com.mongodb.mongo2bq;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Class to cache the generated Protobuf schema information
 */
public class CachedProtoSchema {
	// final FileDescriptor fileDescriptor;
	final Descriptor messageDescriptor;
	final ProtoSchema protoSchema;

	CachedProtoSchema(Descriptor messageDescriptor) {
		// this.fileDescriptor = fileDescriptor;
		this.messageDescriptor = messageDescriptor;
		this.protoSchema = ProtoSchema.newBuilder().setProtoDescriptor(messageDescriptor.toProto()).build();
	}

	public boolean containsField(String fieldName) {
		Set<String> knownFields = messageDescriptor.getFields().stream().map(FieldDescriptor::getName)
				.collect(Collectors.toSet());
		if (knownFields.contains(fieldName)) {
			return true;
		}
		return false;

	}

	public ProtoSchema getProtoSchema() {
		return protoSchema;
	}
}