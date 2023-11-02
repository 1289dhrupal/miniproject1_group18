package le.aca.miniproject.helper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetReader.Builder
import org.apache.parquet.avro.AvroParquetReader
import org.apache.avro.generic.GenericRecord
import groovy.json.JsonOutput

def convertParquetToJson(String inputFilePath, String outputFilePath) {
	println "start ${outputFilePath}"

	Path inputFile = new Path(inputFilePath)
	Configuration configuration = new Configuration()
	ParquetReader < GenericRecord > reader = AvroParquetReader.builder(inputFile).withConf(configuration).build()
	GenericRecord record

	FileWriter fileWriter = new FileWriter(outputFilePath);

	int counter = 0;

	fileWriter.write("[");
	String sep = "";
	while ((record = reader.read()) != null) {
		fileWriter.write(sep);
		fileWriter.write(record.toString());
		sep = ", "

		if (++counter % 100 == 0) {
			fileWriter.flush();
			// break; // TODO: This is for debugging purpose only
		}
	}
	fileWriter.write("]");

	fileWriter.flush();
	fileWriter.close();

	println "done ${outputFilePath}"
}

def cabTypeList = [
	// "yellow",
	"green",
	// "fhv",
	// "fhvhv"
]

def basepath = "src/main/resources"

def yearMonth = "2023-01"

cabTypeList.forEach { cabType ->

	def inputFilePath = "${basepath}/${cabType}_tripdata_${yearMonth}.parquet"
	def outputFilePath = "${basepath}/${cabType}_tripdata_${yearMonth}.json"

	convertParquetToJson(inputFilePath, outputFilePath)
}
