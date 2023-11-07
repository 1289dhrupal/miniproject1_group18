package le.aca.miniproject.helpers

import org.bson.Document

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase

import groovy.json.JsonOutput

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
			mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.CLUSTER}.${properties.SERVER}.mongodb.net/${properties.DB}?retryWrites=true&w=majority")
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


		List<Object> list = JsonHelper.loadJson(jsonFilePath)
		MongoCollection<Document> collection = getCollection(collectionName)

		List<Object> documentsBatch = []

		list.eachWithIndex { obj, idx ->
			Document doc = Document.parse(JsonOutput.toJson(obj))
			documentsBatch.add(doc)

			// When the batch size is reached, insert the batch and clear the list
			if (documentsBatch.size() == 100_000 || idx + 1 == list.size()) {
				collection.insertMany(documentsBatch)
				documentsBatch = []
			}
		}
	}
}
