package lt.mk.awskeyspacebackuptos3.csv;

import static java.util.Arrays.asList;
import static lt.mk.awskeyspacebackuptos3.csv.CsvLine.csvLineParse;
import static lt.mk.awskeyspacebackuptos3.csv.CsvLine.csvLinesParse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CsvLineTest {

	@Test
	void test() {
		assertEquals(asList("a", "b"), csvLineParse("a,b"));
		assertEquals(asList("a", "b", ""), csvLineParse("\"a\",b,"));
		assertEquals(asList("I'a good, good.", "", ""), csvLineParse("\"I'a good, good.\",,"));
	}

	@Test
	void testcsvLinesParse() {
		assertEquals(asList(
				asList("a", "b"),
				asList("a", "b", ""),
				asList("I'a good, good.", "", "")
		), csvLinesParse(asList(
				"a,b",
				"\"a\",b,",
				"\"I'a good, good.\",,"
		)));
	}
}
