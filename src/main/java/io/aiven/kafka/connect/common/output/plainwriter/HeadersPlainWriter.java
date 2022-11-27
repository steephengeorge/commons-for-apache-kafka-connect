/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.output.plainwriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;


public class HeadersPlainWriter implements OutputFieldPlainWriter {
    private static final byte[] HEADER_KEY_VALUE_SEPARATOR = ":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADERS_SEPARATOR = ";".getBytes(StandardCharsets.UTF_8);

    @Override
    public void write(final SinkRecord record, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        for (final Header header : record.headers()) {
            final String topic = record.topic();
            final String key = header.key();
            final Object value = header.value();
            final Schema schema = header.schema();
            outputStream.write(Base64.getEncoder().encode(key.getBytes()));
            outputStream.write(HEADER_KEY_VALUE_SEPARATOR);
            byte[] headerValueBytes = null;
            if (schema != null && schema.type() != null && schema.type().isPrimitive()) {
                headerValueBytes = convertHeader(value, schema);
            }
            outputStream.write(Base64.getEncoder().encode(headerValueBytes));
            outputStream.write(HEADERS_SEPARATOR);
        }
    }
    private byte[] convertHeader(final Object value, final Schema schema) {
        byte[] result = null;
        switch (schema.type()) {
            case INT8: {
                result = Ints.toByteArray((byte) value);
                break;
            }

            case INT16: {
                result = Ints.toByteArray((short) value);
                break;
            }

            case INT32: {
                result = Ints.toByteArray((Integer) value);
                break;
            }
            case INT64: {
                result = Longs.toByteArray((long) value);
                break;
            }

            case FLOAT32: {
                result = ByteBuffer.allocate(8).putFloat((Float) value).array();
                break;
            }
            case FLOAT64: {
                result = ByteBuffer.allocate(16).putDouble((Double) value).array();
                break;
            }
            case BOOLEAN: {
                result = ByteBuffer.allocate(1).put((byte) ((Boolean) value ? 1 : 0)).array();
                break;
            }
            case STRING: {
                result = ((String) value).getBytes(StandardCharsets.UTF_8);
                break;
            }
            case BYTES: {
                result = (byte[]) value;
                break;
            }
            default: {

            }
        }
        return result;
    }
}
