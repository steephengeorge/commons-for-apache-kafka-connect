package io.aiven.kafka.connect.common.output.plainwriter;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

public class HeaderPlainWriterTest {
    private final OutputStream outputStream = new OutputStream() {

        @Override
        public void write(int b) throws IOException {
            System.out.println(b);
        }
    };

    @Test
    void writeTest() throws IOException{
        final HeadersPlainWriter writer = new HeadersPlainWriter();
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addBoolean("key1", Boolean.TRUE); // Boolean
        headers.addString("key2", "value1"); //String
        byte a = 1;
        headers.addByte("key3", a);
        headers.addBytes("key4", "value".getBytes()); //Bytes
        headers.addInt("key8", 9); // Int
        headers.addLong("key9", 0L);
        headers.addShort("key10", (short)1);
        headers.addFloat("key11", 10); //Float
        headers.addDouble("key12", 10.0);
        final SinkRecord sinkRecord = new SinkRecord("testTopic",
            0, null,
            "key_1",
            null,
            "value_1",
            new Random().nextLong(),
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME,
            headers
        );
        writer.write(sinkRecord, outputStream);
    }
}
