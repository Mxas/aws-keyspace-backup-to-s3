package lt.mk.awskeyspacebackuptos3.statistic;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

public class StringFormatter {

	public static String ofBool(boolean b) {
		return String.format("%5s", b);
	}

	public static String ofNum(long val, int size) {
		return String.format("%" + size + "d", val);
	}

	public static String ofRate(double val) {
		return String.format("%8.1f", val);
	}

	public static String ofStr(String val, int size) {
		return String.format("%" + size + "s", val);
	}

	public static String ofBytes(long val) {
		return ofStr(byteSize(val), 10);
	}

	public static String byteSize(long bytes) {
		if (-1000 < bytes && bytes < 1000) {
			return bytes + " B";
		}
		CharacterIterator ci = new StringCharacterIterator("kMGTPE");
		while (bytes <= -999_950 || bytes >= 999_950) {
			bytes /= 1000;
			ci.next();
		}
		return String.format("%.1f %cB", bytes / 1000.0, ci.current());
	}
}
