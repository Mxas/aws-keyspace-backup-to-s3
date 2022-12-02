/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lt.mk.awskeyspacebackuptos3.keyspace.codecs;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.util.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Instant;
import javax.annotation.Nullable;

public class StringToTimestampCodec implements TypeCodec<String> {

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

	@Nullable
	@Override
	public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
		return (value == null) ? null : inner.encode(inner.parse(Strings.quote(value)), protocolVersion);
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
