package le.aca.miniproject;

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import le.aca.miniproject.helpers.JsonHelper


/**
 * Filter trips with trip_miles, trip_time greater than 0 and base_passenger greater than equal to 0.
 * Select columns 
 * [hvfhs_license_num, dispatching_base_num, trip_miles, trip_time, base_passenger_fare, tips, driver_pay]
 * Group By and  hvfhs_license_num#dispatching_base_num
 * Aggreagate rest of the fields
 * Sort in desc of total trips
 */

// Read Properties From File
def properties = new Properties()
def propertiesFile = new File("src/main/resources/application.properties")
propertiesFile.withInputStream { properties.load(it) }

// Extract Required Properties
def fileName = "tripdata_fhvhv-${properties.data_year_month}-${properties.data_day}"

// From JSON file to Groovy map
def tripList = JsonHelper.loadJson("src/main/resources/${fileName}.json")

def licenseToServiceMap = [
	'HV0002': 'Juno',
	'HV0003': 'Uber',
	'HV0004': 'Via',
	'HV0005': 'Lyft'
]

def projection = {
	[
		hvfhs_license_num: it.hvfhs_license_num, // HV0002: Juno; HV0003: Uber; HV0004: Via; HV0005: Lyft;
		dispatching_base_num: it.dispatching_base_num, // Area of the base

		trip_miles: it.trip_miles, // Total Miles for trip
		trip_time: it.trip_time, // Time taken for the trip
		base_passenger_fare: it.base_passenger_fare, // How much the base passenger pay is

		tips: it.tips, // Tips over and above the fare
		driver_pay: it.driver_pay // How much the driver was paid
	]
}


def selectedTrips = tripList
		.findAll{it.trip_miles > 0 && it.trip_time > 0 && it.base_passenger_fare >= 0}
		.collect{projection(it)}
		.groupBy { licenseToServiceMap.get(it.hvfhs_license_num, 'Unknown') + "#" + it.dispatching_base_num }
		.collect {  baseNum, trips ->
			[
				company_base: baseNum,
				trips_count: trips.size(),
				avg_trip_miles: trips*.trip_miles.sum() / trips.size(),
				avg_trip_time: trips*.trip_time.sum() / trips.size(),
				total_base_fare: trips*.base_passenger_fare.sum(),
				avg_tips: trips*.tips.sum() / trips.size(),
				avg_driver_pay: trips*.driver_pay.sum() / trips.size()
			]
		}
		.sort { it.trips_count }
		.reverse()

println(JsonOutput.prettyPrint(JsonOutput.toJson(selectedTrips)))
