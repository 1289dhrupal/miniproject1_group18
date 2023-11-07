package le.aca.miniproject;

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

// from JSON file to Groovy map
def jsonSlurper = new JsonSlurper()

def file = new File('src/main/resources/parsed_input-fhvhv-2023-01-01.json')
def tripList = jsonSlurper.parseText(file.text)


/**
 * Example of simple query:
 *
 * Select movies with more than a 9 rating
 * and project title and year.
 * Then the resulting movies are sorted by year, in ascending order.
 */
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
		.collectEntries {  baseNum, trips ->
			[
				(baseNum): [
					avg_trip_miles: trips*.trip_miles.sum() / trips.size(),
					avg_trip_time: trips*.trip_time.sum() / trips.size(),
					total_base_fare: trips*.base_passenger_fare.sum(),
					avg_tips: trips*.tips.sum() / trips.size(),
					company_base: baseNum
				]
			]
		}

println(JsonOutput.prettyPrint(JsonOutput.toJson(selectedTrips)))


