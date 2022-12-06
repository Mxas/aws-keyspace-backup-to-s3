package lt.mk.awskeyspacebackuptos3.keyspace.codecs;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

import java.math.BigDecimal;
import java.math.BigInteger;

public class CodecsHolder {


	public static TypeCodec<?>[] getAdditionalCodecs() {
		return new TypeCodec<?>[]{
				new StringToLocalDateCodec(),
				new StringToTimestampCodec(),
				new AbstractStringToCodec<>(DataTypes.INT, TypeCodecs.INT, Integer::valueOf),
				new AbstractStringToCodec<>(DataTypes.FLOAT, TypeCodecs.FLOAT, Float::valueOf),
				new AbstractStringToCodec<>(DataTypes.TINYINT, TypeCodecs.TINYINT, Byte::valueOf),
				new AbstractStringToCodec<>(DataTypes.BIGINT, TypeCodecs.BIGINT, Long::valueOf),
				new AbstractStringToCodec<>(DataTypes.COUNTER, TypeCodecs.COUNTER, Long::valueOf),
				new AbstractStringToCodec<>(DataTypes.SMALLINT, TypeCodecs.SMALLINT, Short::valueOf),
				new AbstractStringToCodec<>(DataTypes.DECIMAL, TypeCodecs.DECIMAL, BigDecimal::new),
				new AbstractStringToCodec<>(DataTypes.DOUBLE, TypeCodecs.DOUBLE, Double::valueOf),
				new AbstractStringToCodec<>(DataTypes.VARINT, TypeCodecs.VARINT, BigInteger::new),
				new AbstractStringToCodec<>(DataTypes.BOOLEAN, TypeCodecs.BOOLEAN, Boolean::valueOf)
		};
	}
}
