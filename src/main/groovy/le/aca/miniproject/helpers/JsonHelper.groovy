package le.aca.miniproject.helpers

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

public class JsonHelper {

	public static saveParquetFromURL (parquetFileUrl, parquetFilePath) {
		BufferedInputStream ip
		FileOutputStream fos

		try  {
			ip = new BufferedInputStream(new URL(parquetFileUrl).openStream())
			fos = new FileOutputStream(parquetFilePath)

			byte[] dataBuffer = new byte[1024]
			int bytesRead

			while ((bytesRead = ip.read(dataBuffer, 0, 1024)) != -1) {
				fos.write(dataBuffer, 0, bytesRead);
			}
		} catch (IOException e) {
			System.out.println("Failed to store Parquet file.")
			System.exit(0)
		} finally {
			ip.close();
			fos.close();
		}
	}

	public static int saveParquetToJson(String parquetFilePath, String jsonFilePath, Closure<Boolean> skipRecord, Closure<Map<String, Object>> projectRecordToJson) {
		Path parquetPath = new Path(parquetFilePath)
		FileWriter fileWriter
		try {
			HadoopInputFile parquetFile = HadoopInputFile.fromPath(parquetPath , new Configuration());
			ParquetReader < GenericRecord > reader = AvroParquetReader.builder(parquetFile).build()

			fileWriter = new FileWriter(jsonFilePath)
			int counter = 0
			String sep = ""

			GenericRecord record
			fileWriter.write("[")
			while ((record = reader.read()) != null) {

				if(skipRecord(record)) {
					continue
				}

				String jsonRecord = JsonOutput.toJson(projectRecordToJson(record))
				fileWriter.write("${sep}${jsonRecord}")
				sep = ","

				counter++;
			}

			fileWriter.write("]")
			fileWriter.flush()

			return counter;
		} catch (IOException e) {
			System.out.println("Failed to store Json file.")
			System.exit(0)
		} finally {
			fileWriter.close()
		}
	}

	public static List<Object> loadJson(String jsonFilePath) {
		File jsonFile = new File(jsonFilePath)
		JsonSlurper jsonSlurper = new JsonSlurper()
		return jsonSlurper.parseText(jsonFile.text)
	}
}