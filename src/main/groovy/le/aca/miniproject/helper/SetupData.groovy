package le.aca.miniproject.helper

import java.text.SimpleDateFormat

import org.apache.avro.generic.GenericRecord

import groovy.transform.Field

@Field final BASE_DIR_PATH = '''src/main/resources/%s'''
@Field final BASE_URL = '''https://d37ci6vzurychx.cloudfront.net/trip-data/%s_tripdata_%s.parquet'''

def convertParquetToJson(String tripType, String YearMonth, String Day) {

	String parquetFileUrl = String.format(BASE_URL, tripType, YearMonth)
	String parquetFilePath = String.format(BASE_DIR_PATH, "raw_input-${tripType}-${YearMonth}-${Day}.parquet")
	String jsonFilePath = String.format(BASE_DIR_PATH, "parsed_input-${tripType}-${YearMonth}-${Day}.json")

	Closure<Map<String, Object>> projectRecordToJson = { GenericRecord record ->
		return [
			hvfhs_license_num: record.get("hvfhs_license_num"),
			dispatching_base_num: record.get("dispatching_base_num"),

			trip_miles: record.get("trip_miles"),
			trip_time: record.get("trip_time"),

			base_passenger_fare: record.get("base_passenger_fare"),
			tips: record.get("tips"),
			driver_pay: record.get("driver_pay")
		]
	}


	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd")
	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
	long epochStart = dateFormat.parse("${YearMonth}-${Day}").time
	long epochEnd = (60 * 60 * 24 * 1000) + epochStart

	Closure<Boolean> filterRecord = { GenericRecord record ->
		record.get("request_datetime")/1000 < epochStart || record.get("request_datetime")/1000 >= epochEnd
	}


	if (!new File(jsonFilePath).exists()) {
		if(!new File(parquetFilePath).exists()) {
			JsonHelper.saveParquetFromURL(parquetFileUrl, parquetFilePath)
		}

		int savedObjects = JsonHelper.saveParquetToJson(parquetFilePath, jsonFilePath, filterRecord, projectRecordToJson)
		assert savedObjects != 0
	}

	return jsonFilePath
}

def upsertJsonToMongoDB(String propertiesFile, String jsonFile, String collectionName) {
	String propertiesFilePath = String.format(BASE_DIR_PATH, propertiesFile)

	MongoDBHelper mdb = new MongoDBHelper(propertiesFilePath)
	if(mdb.getCollection(collectionName).countDocuments() == 0) {
		mdb.upsertFile(jsonFile, collectionName)
	}

	mdb.close()
}

// As of now we are only using fhvhv trip data for the month of Jan 2023
// This will be done by reading the whole Jan Data and filtering for day 01 while creating the json.
// This is done due to the storage and computing restrictions
// Parsed 629_770 objects and saved at location: src/main/resources/parsed_input-fhvhv-2023-01-01.json

String tripType = "fhvhv"
String yearMonth = "2023-01"
String day = "01"

String jsonFilePath = convertParquetToJson(tripType, yearMonth, day)
upsertJsonToMongoDB("mongodb.properties", jsonFilePath, "tripdata_${tripType}")


