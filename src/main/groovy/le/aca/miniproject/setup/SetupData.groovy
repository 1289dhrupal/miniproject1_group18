package le.aca.miniproject.setup

import java.text.SimpleDateFormat

import org.apache.avro.generic.GenericRecord

import groovy.transform.Field
import le.aca.miniproject.helpers.JsonHelper
import le.aca.miniproject.helpers.MongoDBHelper

@Field static final BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_%s.parquet"

public static String saveTripData(Properties pProperties) {
	String lstrparquetFileUrl = String.format(BASE_URL, pProperties.data_year_month)
	String lstrparquetFilePath = "src/main/resources/tripdata_fhvhv-${pProperties.data_year_month}.parquet"
	String lstrJsonFilePath = "src/main/resources/tripdata_fhvhv-${pProperties.data_year_month}-${pProperties.data_day}.json"

	Closure<Map<String, Object>> lmapProjectRecordToJson = { GenericRecord record ->
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

	SimpleDateFormat lDateFormat = new SimpleDateFormat("yyyy-MM-dd")
	lDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

	long llStart = lDateFormat.parse("${pProperties.data_year_month}-${pProperties.data_day}").time
	long llEnd = (60 * 60 * 24 * 1000) + llStart

	Closure<Boolean> lskipRecordClosure = { GenericRecord record ->
		long llRequestDatetime = record.get("request_datetime")/1000;
		llRequestDatetime < llStart || llRequestDatetime >= llEnd
	}

	if (!new File(lstrJsonFilePath).exists()) {
		if(!new File(lstrparquetFilePath).exists()) {
			JsonHelper.saveParquetFromURL(lstrparquetFileUrl, lstrparquetFilePath)
		}

		int liSavedObjects = JsonHelper.saveParquetToJson(lstrparquetFilePath, lstrJsonFilePath, lskipRecordClosure, lmapProjectRecordToJson)
		assert liSavedObjects != 0
	}

	return lstrJsonFilePath
}

public static void upsertMongoDB(String pJsonFilePath, Properties pProperties) {
	String lstrCollectionName = "tripdata_fhvhv-${pProperties.data_year_month}-${pProperties.data_day}"
	MongoDBHelper lMongodbHelper = new MongoDBHelper(pProperties)
	if(lMongodbHelper.getCollection(lstrCollectionName).countDocuments() == 0) {
		lMongodbHelper.upsertFile(pJsonFilePath, lstrCollectionName)
	}
	lMongodbHelper.close()
}

// As of now we are only using fhvhv trip data for the month of Jan 2023
// This will be done by reading the whole Jan Data and filtering for day 01 while creating the json.
// This is done due to the storage and computing restrictions
// Parsed 629_770 objects and saved at location: src/main/resources/parsed_input-fhvhv-2023-01-01.json

// Read Properties From File
Properties lProperties = new Properties()
File lPropertiesFile = new File("src/main/resources/application.properties")
lPropertiesFile.withInputStream { lProperties.load(it) }

// Fetch Parquet File From the CDN and save to Json File and MongoDB
String lstrJsonFilePath = saveTripData(lProperties)
upsertMongoDB(lstrJsonFilePath, lProperties)
