package lt.mk.awskeyspacebackuptos3.keyspace;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.File;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
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
//				.addTypeCodecs(new DateCodec1(TypeCodecs.DATE))
//				.addTypeCodecs(new IntCodec1(TypeCodecs.INT))
//				.addTypeCodecs(new TimestampCodec1())
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

/*

class DateCodec1 implements TypeCodec<String> {

	private final TypeCodec<LocalDate> innerCodec;

	public DateCodec1(TypeCodec<LocalDate> codec) {
		innerCodec = codec;
	}


	@NonNull
	@Override
	public GenericType<String> getJavaType() {
		return GenericType.STRING;
	}

	@NonNull
	@Override
	public DataType getCqlType() {
		return DataTypes.DATE;
	}


	@Nullable
	@Override
	public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
		if (value == null) {
			return null;
		}
		return innerCodec.encode(LocalDate.parse(value), protocolVersion);
	}

	@Nullable
	@Override
	public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
		LocalDate p = innerCodec.decode(bytes, protocolVersion);
		if (p == null) {
			return null;
		}
		return p.toString();
	}

	@NonNull
	@Override
	public String format(@Nullable String value) {
		if (value == null) {
			return "NULL";
		}
		LocalDate parse = LocalDate.parse(value);
		return innerCodec.format(parse);
	}

	@Nullable
	@Override
	public String parse(@Nullable String value) {
		if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
			return null;
		}
		LocalDate parse = innerCodec.parse(value);
		return parse == null ? null : parse.toString();
	}
}

class IntCodec1 implements TypeCodec<String> {

	private final PrimitiveIntCodec inner;

	IntCodec1(PrimitiveIntCodec inner) {
		this.inner = inner;
	}

	@NonNull
	@Override
	public GenericType<String> getJavaType() {
		return GenericType.STRING;
	}

	@NonNull
	@Override
	public DataType getCqlType() {
		return DataTypes.INT;
	}

	@Override
	public boolean accepts(@NonNull Object value) {
		return value instanceof Integer;
	}

	@Nullable
	@Override
	public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
		return (value == null) ? null : inner.encode(Integer.valueOf(value), protocolVersion);
	}

	@Nullable
	@Override
	public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
		Integer decode = inner.decode(bytes, protocolVersion);
		return decode == null ? null : decode.toString();
	}

	@NonNull
	@Override
	public String format(@Nullable String value) {
		return (value == null) ? "NULL" : value;
	}

	@Nullable
	@Override
	public String parse(@Nullable String value) {
		Integer parse = inner.parse(value);
		return parse == null ? null : parse.toString();
	}


}

class TimestampCodec1 implements TypeCodec<String> {

	private final TimestampCodec inner = new TimestampCodec();


	@NonNull
	@Override
	public GenericType<String> getJavaType() {
		return GenericType.STRING;
	}

	@NonNull
	@Override
	public DataType getCqlType() {
		return DataTypes.TIMESTAMP;
	}

	@Override
	public boolean accepts(@NonNull Object value) {
		return value instanceof Integer;
	}

	@Nullable
	@Override
	public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
		return (value == null) ? null : inner.encode(inner.parse(value), protocolVersion);
	}

	@Nullable
	@Override
	public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
		Instant decode = inner.decode(bytes, protocolVersion);
		return decode == null ? null : decode.toString();
	}

	@NonNull
	@Override
	public String format(@Nullable String value) {
		return (value == null) ? "NULL" : value;
	}

	@Nullable
	@Override
	public String parse(@Nullable String value) {
		Instant parse = inner.parse(value);
		return parse == null ? null : parse.toString();
	}
}
*/




