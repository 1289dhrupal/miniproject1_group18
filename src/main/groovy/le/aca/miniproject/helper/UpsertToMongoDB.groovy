package le.aca.miniproject.helper

import org.bson.Document

import com.mongodb.client.MongoClients

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

System.exit(0)

def properties = new Properties()
def propertiesFile = new File('src/main/resources/mongodb.properties')

propertiesFile.withInputStream {
	properties.load(it)
}

// MAKING THE CONNECTION
def mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.SERVER}.mongodb.net/${properties.DB}?retryWrites=true&w=majority")

// GET DATABASE
def db = mongoClient.getDatabase(properties.DB)

// TESTING CONNECTION
println 'database: ' + db.getName()
db.listCollectionNames().each{ println it }

def collection = db.getCollection("taxi-data")
collection.drop()

println "==========================================================================================="

// parse JSON file
def jsonFile = new File('src/main/resources/parsed_input.json')
def jsonSlurper = new JsonSlurper()
def list = jsonSlurper.parseText(jsonFile.text)

println "==========================================================================================="

def documentsBatch = []
def batchSize = 10000

for (obj in list) {
	def doc = Document.parse(JsonOutput.toJson(obj))
	documentsBatch.add(doc)

	// When the batch size is reached, insert the batch and clear the list
	if (documentsBatch.size() == batchSize) {
		collection.insertMany(documentsBatch)
		println "Inserted : ${documentsBatch.size()}"
		documentsBatch = []
	}
}

if (documentsBatch.size() > 0) {
	collection.insertMany(documentsBatch)
	println "Inserted : ${documentsBatch.size()}"
	documentsBatch = []
}

println "Total Documents Inserted : ${collection.countDocuments()}"

println "==========================================================================================="

// Close the MongoDB connection
mongoClient.close()


