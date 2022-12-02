package lt.mk.awskeyspacebackuptos3.csv;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvLine {

	public static List<String> csvLineParse(String line) {
		try (CSVParser parse = CSVParser.parse(line, CSVFormat.DEFAULT)) {
			return parse.getRecords().get(0).toList();
		} catch (Exception e) {
			throw new RuntimeException("Failed parse line: " + line, e);
		}
	}

	public static List<List<String>> csvLinesParse(List<String> line) {
		try (CSVParser parse = CSVParser.parse(new BufferedReader(new StringReader(String.join("\n", line))), CSVFormat.DEFAULT)) {
			return parse.getRecords().stream().map(CSVRecord::toList).collect(Collectors.toList());
		} catch (Exception e) {
			throw new RuntimeException("Failed parse line: " + line, e);
		}
	}
}
