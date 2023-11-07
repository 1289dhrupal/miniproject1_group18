package le.aca.miniproject.helper

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import groovy.json.JsonOutput

def getEpochTimeInMicrosecondsForDate(date) {
	def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
	return dateFormat.parse(date).time * 1000 // Convert milliseconds to microseconds
}

def convertParquetToJson(tripType, Year, Month, Day) {

	def basepath = "src/main/resources"

	try  {
		def ip = new BufferedInputStream(new URL("https://d37ci6vzurychx.cloudfront.net/trip-data/${tripType}_tripdata_${Year}-${Month}.parquet").openStream())
		def fos = new FileOutputStream("${basepath}/raw_input.parquet")
		def dataBuffer = new byte[1024]
		int bytesRead;

		while ((bytesRead = ip.read(dataBuffer, 0, 1024)) != -1) {
			fos.write(dataBuffer, 0, bytesRead);
		}

		ip.close();
		fos.close();
	} catch (IOException e) {
		// handle exception
	} finally {
	}

	def inputFilePath = "${basepath}/raw_input.parquet"
	def outputFilePath = "${basepath}/parsed_input.json"
	def epochStart = getEpochTimeInMicrosecondsForDate("${Year}-${Month}-${Day}")
	def epochEnd = getEpochTimeInMicrosecondsForDate("${Year}-${Month}-${Day.toInteger() + 1}")

	def project = { it ->

		return [
			hvfhs_license_num: it.get("hvfhs_license_num"),
			dispatching_base_num: it.get("dispatching_base_num"),

			trip_miles: it.get("trip_miles"),
			trip_time: it.get("trip_time"),

			base_passenger_fare: it.get("base_passenger_fare"),
			tips: it.get("tips"),
			driver_pay: it.get("driver_pay")
		]
	}

	println "start ${inputFilePath} from ${epochStart} to ${epochEnd}"

	def inputPath = new Path(inputFilePath)
	def inputFile = HadoopInputFile.fromPath(inputPath , new Configuration());

	def reader = AvroParquetReader.builder(inputFile).build()
	def record

	def fileWriter = new FileWriter(outputFilePath)

	int counter = 0
	def sep = ""

	fileWriter.write("[")
	while ((record = reader.read()) != null) {

		if (record.get("request_datetime") < epochStart || record.get("request_datetime") >= epochEnd) {
			continue
		}

		// def jsonRecord = record.toString();

		def projectedRecord = project(record)
		def jsonRecord = JsonOutput.toJson(projectedRecord)
		fileWriter.write("${sep}${jsonRecord}")
		sep = ","

		counter++;
	}

	fileWriter.write("]")

	fileWriter.flush()
	fileWriter.close()

	println "Parsed ${counter} objects and saved at location: ${outputFilePath}"
}

// As of now we are only using fhvhv trip data for the month of Jan 2023 01
// This will be done by reading the whole Jan Data and filtering while creating the json.
// Parsed 629_770 objects and saved at location: src/main/resources/fhvhv_tripdata_2023-01-01.json
def tripType = "fhvhv"
def Year = "2023"
def Month = "01"
def Day = "01"

convertParquetToJson(tripType, Year, Month, Day)
