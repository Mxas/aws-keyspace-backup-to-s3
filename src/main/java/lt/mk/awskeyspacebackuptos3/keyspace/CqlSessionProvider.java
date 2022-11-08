package lt.mk.awskeyspacebackuptos3.keyspace;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;

public class CqlSessionProvider {

	private final AwsKeyspaceConf conf;
	private CqlSession session;

	public CqlSessionProvider(AwsKeyspaceConf conf) throws NoSuchAlgorithmException {
		this.conf = conf;
	}

	private void initSession() {
		this.session = CqlSession.builder()
				.withConfigLoader(DriverConfigLoader.fromFile(getFile()))
				.withSslContext(getaDefault())
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
		File file = new File(this.conf.awsKeyspaceDriverConfigPath);
		if (!file.isFile() || !file.exists()) {
			throw new IllegalArgumentException("Keyspace config file not found: " + this.conf.awsKeyspaceDriverConfigPath);
		}
		return file;
	}

	public void close() {
		try {
			if (session != null) {
				session.close();
				session = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public CqlSession getSession() {
		if (session == null) {
			initSession();
		}
		return session;
	}
}




