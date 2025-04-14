package com.mongodb.mongo2bq;

import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;

public class MongoHelper {
	
	public static Document runCommand(MongoClient mongoClient, Document command, String dbName) {
		return mongoClient.getDatabase(dbName).runCommand(command);
	}
	
	public static Document getCollectionInfo(MongoClient mongoClient, Namespace ns) {
		
		Document filter = new Document("name", ns.getCollectionName());
		Document command = new Document("listCollections", 1).append("filter", filter);
		
		Document outer = runCommand(mongoClient, command, ns.getDatabaseName());
		Document cursor = (Document)outer.get("cursor");
		
		List<Document> firstBatch = cursor.getList("firstBatch", Document.class);
		
		if (firstBatch.isEmpty()) {
			throw new IllegalArgumentException("listCollections was not able to find collectionInfo for namespace " + ns);
		}
		
		return firstBatch.get(0);
	}

}
