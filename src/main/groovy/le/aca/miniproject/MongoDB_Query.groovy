package le.aca.miniproject

import static com.mongodb.client.model.Accumulators.*
import static com.mongodb.client.model.Aggregates.*
import static com.mongodb.client.model.Filters.*
import static com.mongodb.client.model.Projections.*
import static com.mongodb.client.model.Sorts.*

import org.bson.Document

import groovy.json.JsonOutput
import le.aca.miniproject.helpers.MongoDBHelper

// Read Properties From File
def properties = new Properties()
def propertiesFile = new File("src/main/resources/application.properties")
propertiesFile.withInputStream { properties.load(it) }

// Extract Required Properties
def collectionName = "tripdata_fhvhv-${properties.data_year_month}-${properties.data_day}"

/**
 * match trips with trip_miles, trip_time greater than 0 and base_passenger greater than equal to 0.
 * group by hvfhs_license_num#dispatching_base_num and aggreagate required fields
 * Replace the id as company_name#dispatching_base_num 
 * Sort in desc of total trips
 */

// Initialize the MongoDB client
def mdb = new MongoDBHelper(properties)

// Get the Collection
def collection = mdb.getCollection(collectionName)

def replace = { a, b ->
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
        sum("trips_count", 1),
        avg("avg_trip_miles", "\$trip_miles"),
        avg("avg_trip_time", "\$trip_time"),
        sum("total_base_fare", "\$base_passenger_fare"),
        avg("avg_tips", "\$tips"),
        avg("avg_driver_pay", "\$driver_pay"),
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
            include("trips_count", "avg_trip_miles", "avg_trip_time", "total_base_fare", "avg_tips", "avg_driver_pay"),
        )
    ),
    sort(descending("trips_count"))
]

def selectedTrips = collection.aggregate(pipeline).into(new ArrayList < > ())
println(JsonOutput.prettyPrint(JsonOutput.toJson(selectedTrips)))

// Close the MongoDB connection
mdb.close()