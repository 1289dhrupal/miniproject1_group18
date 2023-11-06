package le.aca.miniproject.helper

import org.bson.Document

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.client.MongoClients

import groovy.json.JsonOutput
import groovy.json.JsonSlurper


// load credentials from src/main/resources/mongodb.properties
// this file should contain
//		USN=yourUsername
//		PWD=yourPassword
//		DB=yourDatabaseName

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

def filter = { key, val ->
	def filterAux = new BasicDBObject()
	filterAux.append(key, val)
	filterAux
}

def set = { key, val ->
	def updateObject = new BasicDBObject()
	updateObject.append('$set', new BasicDBObject().append(key,val))
	updateObject
}

def collection = db.getCollection("taxi-data")

println "==========================================================================================="

// parse JSON file
def jsonFile = new File('src/main/resources/fhvhv_tripdata_2023-01.json')
def jsonSlurper = new JsonSlurper()
def list = jsonSlurper.parseText(jsonFile.text)

for (obj in list) {
	def doc = Document.parse(JsonOutput.toJson(obj))
	collection.insertOne(doc)
}

println "==========================================================================================="

println collection .countDocuments()

println "==========================================================================================="

// Close the MongoDB connection
mongoClient.close()


