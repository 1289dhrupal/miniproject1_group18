package le.aca.miniproject.sample

import org.bson.Document


import com.mongodb.client.MongoClients

import static com.mongodb.client.model.Filters.*
import static com.mongodb.client.model.Projections.*
import static com.mongodb.client.model.Sorts.*
import static com.mongodb.client.model.Accumulators.*

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import static com.mongodb.client.model.Aggregates.*

// load credentials from src/main/resources/mongodb.properties
// this file should contain 
//		USN=yourUsername
//		PWD=yourPassword
//		DATABASE=yourDatabaseName 
def properties = new Properties()
def propertiesFile = new File('src/main/resources/mongodb.properties')
propertiesFile.withInputStream {
	properties.load(it)
}

// parse JSON file
def jsonFile = new File('src/main/resources/top-rated-movies.json')
def jsonSlurper = new JsonSlurper()
def list = jsonSlurper.parseText(jsonFile.text)

// create connection and upload contents
def mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.CLUSTER}.${properties.HOST}.mongodb.net/${properties.DB}?retryWrites=true&w=majority");
def db = mongoClient.getDatabase(properties.DB);
def col = db.getCollection("movies")
col.drop()

for (obj in list) {
	def doc = Document.parse(JsonOutput.toJson(obj))
	col.insertOne(doc)
}

/**
 * Example of query that requires using MongoDb Java Driver in more depth.
 *
 * Select actors that star more than 1 movie with at least a 9 imdb rating
 */
def resultList = col.aggregate([
	match(gte('imdbRating',9)),
	project(fields(include('title', 'year', 'actors', 'imdbRating'),excludeId())),
	unwind('$actors'),
	group('$actors', sum('count', 1L)),
	match(gt('count',1))
])

println()
println()
resultList.each { println(it) }
println()
println()
