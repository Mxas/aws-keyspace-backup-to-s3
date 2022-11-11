package lt.mk.awskeyspacebackuptos3.keyspace;

public class KeyspaceUtil {

	public static void checkError(Throwable error, int page) {
		if (error != null) {
			System.out.println("Data fetching failed in page: " + page);
			System.out.println(error.getMessage());
			error.printStackTrace();
			throw new RuntimeException("Data fetching failed in page: " + page, error);
		}
	}
}
