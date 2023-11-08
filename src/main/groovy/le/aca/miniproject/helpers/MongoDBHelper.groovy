package le.aca.miniproject.helpers

import org.bson.Document

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase

import groovy.json.JsonOutput

public class MongoDBHelper {

	private Properties mProperties = null;
	private MongoClient mMongoClient = null;

	public MongoDBHelper(Properties pProperties) {
		mProperties = pProperties
	}

	public MongoClient getConnection() {
		if (mMongoClient == null) {
			mMongoClient = MongoClients.create("mongodb+srv://${mProperties.mongodb_usn}:${mProperties.mongodb_password}@${mProperties.mongodb_cluster}.${mProperties.mongodb_server}.mongodb.net/${mProperties.mongodb_database}?retryWrites=true&w=majority")
		}
		return mMongoClient
	}

	public MongoDatabase getDatabase() {
		return getConnection().getDatabase(mProperties.mongodb_database);
	}

	public MongoCollection<Document> getCollection(String pCollectionName) {
		return getDatabase().getCollection(pCollectionName)
	}

	public MongoCollection<Document> dropAndGetCollection(String pCollectionName) {
		MongoCollection<Document> lCollection = getCollection(pCollectionName)
		lCollection.drop()
		return lCollection
	}

	public void close() {
		mMongoClient.close();
		mMongoClient = null
	}

	public void upsertFile(String jsonFilePath, String pCollectionName) {


		List<Object> llistObj = JsonHelper.loadJson(jsonFilePath)
		List<Document> llistDocBatch = []

		llistObj.eachWithIndex { obj, idx ->
			Document lDoc = Document.parse(JsonOutput.toJson(obj))
			llistDocBatch.add(lDoc)

			// When the batch size is reached, insert the batch and clear the list
			if (llistDocBatch.size() == 100_000 || idx + 1 == llistObj.size()) {
				getCollection(pCollectionName).insertMany(llistDocBatch)
				llistDocBatch = []
			}
		}
	}
}
