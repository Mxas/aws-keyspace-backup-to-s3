package lt.mk.awskeyspacebackuptos3.keyspace;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.codecs.CodecsHolder;
import org.apache.commons.lang3.StringUtils;

public class CqlSessionProvider {

	private final AwsKeyspaceConf conf;
	private CqlSession session1;
	private CqlSession session2;

	public CqlSessionProvider(AwsKeyspaceConf conf) throws NoSuchAlgorithmException {
		this.conf = conf;
	}

	public CqlSession createSession() {
		return CqlSession.builder()
				.withConfigLoader(DriverConfigLoader.fromFile(getFile()))
				.withSslContext(getaDefault())
				.addTypeCodecs(CodecsHolder.getAdditionalCodecs())
				.addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
				.build();
	}

	private static SSLContext getaDefault() {
		try {
			return SSLContext.getDefault();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	private File getFile() {
		if (StringUtils.isBlank(this.conf.awsKeyspaceDriverConfigPath)) {
			throw new IllegalArgumentException("Keyspace config file not provided");
		}
		File file = new File(this.conf.awsKeyspaceDriverConfigPath);
		if (!file.isFile() || !file.exists()) {
			throw new IllegalArgumentException("Keyspace config file not found: " + this.conf.awsKeyspaceDriverConfigPath);
		}
		return file;
	}

	public void close() {
		try {
			if (session1 != null) {
				session1.close();
				session1 = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (session2 != null) {
				session2.close();
				session2 = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public CqlSession getReadingSession() {
		if (session1 == null) {
			this.session1 = createSession();
		}
		return session1;
	}

	public void closeReadingSession() {
		if (session1 != null) {
			close(session1);
			this.session1 = null;
		}

	}

	public CqlSession getWriteSession() {
		if (session2 == null) {
			this.session2 = createSession();
		}
		return session2;
	}

	public void closeWriteSession() {
		if (session2 != null) {
			close(session2);
			this.session2 = null;
		}
		System.out.println("Write session closed.");
	}

	public static void close(CqlSession s) {
		try {
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}




