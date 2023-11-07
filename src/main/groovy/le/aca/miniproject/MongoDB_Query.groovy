package le.aca.miniproject

import static com.mongodb.client.model.Accumulators.*
import static com.mongodb.client.model.Aggregates.*
import static com.mongodb.client.model.Filters.*
import static com.mongodb.client.model.Projections.*

import org.bson.Document

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoClients

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
db.listCollectionNames().each { println it }

def collection = db.getCollection("tripdata_fhvhv")

def replace = {
    a,b -> 
	new Document("case", new Document("\$regexMatch", new Document("input", "\$_id").append("regex", a)))
	.append("then", new Document("\$replaceAll", new Document("input", "\$_id").append("find", a).append("replacement", b)))
}

// Aggregation pipeline
def pipeline = [
    match(
        and(
            gt("trip_miles", 0.0),
            gt("trip_time", 0),
            gte("base_passenger_fare", 0.0)
        )
    ),
    group(
        new Document("\$concat", Arrays.asList("\$hvfhs_license_num", "#", "\$dispatching_base_num")),
        avg("avg_trip_miles", "\$trip_miles"),
        avg("avg_trip_time", "\$trip_time"),
        sum("total_base_fare", "\$base_passenger_fare"),
        avg("avg_tips", "\$tips"),
    ),
    project(
        fields(
            excludeId(),
            computed("company_base",
                new Document("\$switch", new Document("branches", Arrays.asList(
                    replace("HV0002", "Juno"),
                    replace("HV0003", "Uber"),
                    replace("HV0004", "Via"),
                    replace("HV0005", "Lyft"),
                )).append("default", "\$_id")),
            ),
            include("avg_trip_miles", "avg_trip_time", "total_base_fare", "avg_tips"),
        )
    )
]
def resultList = collection.aggregate(pipeline).into(new ArrayList < > ())

resultList.each {
    println(it)
}

// Close the MongoDB connection
mongoClient.close()