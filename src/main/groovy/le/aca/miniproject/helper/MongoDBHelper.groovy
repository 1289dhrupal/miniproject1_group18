package le.aca.miniproject.helper

import org.bson.Document

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Field

public class MongoDBHelper {

	Properties properties = null;
	MongoClient mongoClient = null;

	public MongoDBHelper(String propertiesFilePath) {
		properties = new Properties()
		File propertiesFile = new File("${propertiesFilePath}")


		propertiesFile.withInputStream {
			properties.load(it)
		}
	}

	public MongoClient getConnection() {
		if (mongoClient == null) {
			mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.SERVER}.mongodb.net/${properties.DB}?retryWrites=true&w=majority")
		}
		return mongoClient
	}

	public MongoDatabase getDatabase() {
		return getConnection().getDatabase(properties.DB);
	}

	public MongoCollection<Document> getCollection(String collectionName) {
		return getDatabase().getCollection(collectionName)
	}

	public MongoCollection<Document> dropAndGetCollection(String collectionName) {
		MongoCollection<Document> collection = getCollection(collectionName)
		collection.drop()
		return getCollection(collectionName)
	}

	public void close() {
		mongoClient.close();
		mongoClient = null
	}

	public void upsertFile(String jsonFilePath, String collectionName) {

		// parse JSON file
		MongoCollection<Document> collection = getCollection(collectionName)

		File jsonFile = new File("${jsonFilePath}")
		JsonSlurper jsonSlurper = new JsonSlurper()
		def list = jsonSlurper.parseText(jsonFile.text)

		def documentsBatch = []
		int batchSize = 10000

		for (obj in list) {
			Document doc = Document.parse(JsonOutput.toJson(obj))
			documentsBatch.add(doc)

			// When the batch size is reached, insert the batch and clear the list
			if (documentsBatch.size() == batchSize) {
				collection.insertMany(documentsBatch)
				documentsBatch = []
			}
		}

		if (documentsBatch.size() > 0) {
			collection.insertMany(documentsBatch)
			documentsBatch = []
		}
	}
}
