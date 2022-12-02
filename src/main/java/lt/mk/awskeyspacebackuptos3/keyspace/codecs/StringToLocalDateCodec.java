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
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.time.LocalDate;

public class StringToLocalDateCodec implements TypeCodec<String> {

	private final TypeCodec<LocalDate> innerCodec;

	public StringToLocalDateCodec() {
		innerCodec = TypeCodecs.DATE;
	}


	@Override
	public GenericType<String> getJavaType() {
		return GenericType.STRING;
	}

	@Override
	public DataType getCqlType() {
		return DataTypes.DATE;
	}


	@Override
	public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
		if (value == null) {
			return null;
		}
		return innerCodec.encode(LocalDate.parse(value), protocolVersion);
	}

	@Override
	public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
		LocalDate p = innerCodec.decode(bytes, protocolVersion);
		if (p == null) {
			return null;
		}
		return p.toString();
	}

	@Override
	public String format(String value) {
		if (value == null) {
			return "NULL";
		}
		LocalDate parse = LocalDate.parse(value);
		return innerCodec.format(parse);
	}

	@Override
	public String parse(String value) {
		if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
			return null;
		}
		LocalDate parse = innerCodec.parse(value);
		return parse == null ? null : parse.toString();
	}
}
