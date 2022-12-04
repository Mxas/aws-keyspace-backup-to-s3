package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KeyspaceUtil {

	private static final DateTimeFormatter DATE_PATTERN = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");

	public static void checkError(Throwable error, int page) {
		if (error != null) {
			System.out.println("Data fetching failed in page: " + page);
			System.out.println(error.getMessage());
			error.printStackTrace();
			throw new RuntimeException("Data fetching failed in page: " + page, error);
		}
	}

	public static void checkError(Throwable error, int page, AsyncResultSet rs) {
		if (error != null) {
			String message = LocalDateTime.now() + " Data fetching failed in page: " + page;
			message = appendRsData(rs, message);
			System.out.println(message);
			System.out.println(error.getMessage());
			storeException(error, message);
			error.printStackTrace();
			throw new RuntimeException(message, error);
		}
	}

	private static String appendRsData(AsyncResultSet rs, String message) {
		try {
			if (rs != null) {
				message += " remaining" + rs.remaining();
				message += " hasMorePages" + rs.hasMorePages();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		return message;
	}

	private static void storeException(Throwable error, String message) {
		try {
			FileOutputStream fos = null;
			createLogDir();
			File file = new File("logs" + File.separatorChar + "exception_" + DATE_PATTERN.format(LocalDateTime.now()) + ".txt");
			if(file.exists()){
				file.createNewFile();
			}

			fos = new FileOutputStream(file, true);
			PrintStream ps = new PrintStream(fos);
			ps.println(message);
			error.printStackTrace(ps);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createLogDir() {
		var f = new File("logs");
		if (!f.isDirectory()){
			f.mkdir();
		}
	}
}
