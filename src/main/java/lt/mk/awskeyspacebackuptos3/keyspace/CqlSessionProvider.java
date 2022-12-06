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
	private CqlSession session;
	private CqlSession session2;

	public CqlSessionProvider(AwsKeyspaceConf conf) throws NoSuchAlgorithmException {
		this.conf = conf;
	}

	private void initSession() {
		this.session = createSession();
		this.session2 = createSession();
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
			if (session != null) {
				session.close();
				session = null;
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


	public CqlSession getSession() {
		if (session == null) {
			initSession();
		}
		return session;
	}


	public CqlSession getSession2() {
		if (session2 == null) {
			initSession();
		}
		return session2;
	}
}




