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

	public static saveParquetFromURL (pParquetFileUrl, pParquetFilePath) {
		BufferedInputStream lBufferedIS
		FileOutputStream lFileOS

		try  {
			lBufferedIS = new BufferedInputStream(new URL(pParquetFileUrl).openStream())
			lFileOS = new FileOutputStream(pParquetFilePath)

			byte[] larrDataBuffer = new byte[1024]
			int liBytesRead

			while ((liBytesRead = lBufferedIS.read(larrDataBuffer, 0, 1024)) != -1) {
				lFileOS.write(larrDataBuffer, 0, liBytesRead);
			}
		} catch (IOException lException) {
			System.out.println("Failed to store Parquet file." + lException.getMessage())
			System.exit(0)
		} finally {
			lBufferedIS.close();
			lFileOS.close();
		}
	}

	public static int saveParquetToJson(String pParquetFilePath, String pJsonFilePath, Closure<Boolean> pSkipRecordClosure, Closure<Map<String, Object>> pProjectRecordClosure) {
		Path lParquetPath = new Path(pParquetFilePath)
		FileWriter lFileWriter
		try {
			HadoopInputFile lparquetFile = HadoopInputFile.fromPath(lParquetPath , new Configuration());
			ParquetReader < GenericRecord > lReader = AvroParquetReader.builder(lparquetFile).build()

			lFileWriter = new FileWriter(pJsonFilePath)
			int liCounter = 0
			String lstrSep = ""

			GenericRecord lRecord
			lFileWriter.write("[")
			while ((lRecord = lReader.read()) != null) {

				if(pSkipRecordClosure(lRecord)) {
					continue
				}

				String lstrJsonRecord = JsonOutput.toJson(pProjectRecordClosure(lRecord))
				lFileWriter.write("${lstrSep}${lstrJsonRecord}")
				lstrSep = ","

				liCounter++;
			}

			lFileWriter.write("]")
			lFileWriter.flush()

			return liCounter;
		} catch (IOException lException) {
			System.out.println("Failed to store Parquet file." + lException.getMessage())
			System.exit(0)
		} finally {
			lFileWriter.close()
		}
	}

	public static List<Object> loadJson(String pJsonFilePath) {
		File lJsonFile = new File(pJsonFilePath)
		JsonSlurper lJsonSlurper = new JsonSlurper()
		return lJsonSlurper.parseText(lJsonFile.text)
	}
}