package lt.mk.awskeyspacebackuptos3.keyspace.codecs;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

public class StringToBooleanCodec implements TypeCodec<String> {

	private final TypeCodec<Boolean> inner;

	StringToBooleanCodec() {
		this.inner = TypeCodecs.BOOLEAN;
	}

	@NonNull
	@Override
	public GenericType<String> getJavaType() {
		return GenericType.STRING;
	}

	@NonNull
	@Override
	public DataType getCqlType() {
		return DataTypes.BOOLEAN;
	}

	@Nullable
	@Override
	public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
		return (value == null) ? null : inner.encode(Boolean.valueOf(value), protocolVersion);
	}

	@Nullable
	@Override
	public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
		Boolean decode = inner.decode(bytes, protocolVersion);
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
		Boolean parse = inner.parse(value);
		return parse == null ? null : parse.toString();
	}


}
