package com.brightcove.rna.bigtable.avro.io;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class MemcmpDecoderTest {

    @Test
    public void testDecodeInt() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[]{(byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x01});
        Decoder decoder = new MemcmpDecoder(in);
        int i = decoder.readInt();
        assertEquals(1, i);

        in = new ByteArrayInputStream(new byte[]{(byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        decoder = new MemcmpDecoder(in);
        i = decoder.readInt();
        assertEquals(-1, i);

        in = new ByteArrayInputStream(new byte[]{(byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00});
        decoder = new MemcmpDecoder(in);
        i = decoder.readInt();
        assertEquals(0, i);
    }

    @Test
    public void testDecodeLong() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[]{
            (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01});
        Decoder decoder = new MemcmpDecoder(in);
        long i = decoder.readLong();
        assertEquals(1L, i);

        in = new ByteArrayInputStream(new byte[]{
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        decoder = new MemcmpDecoder(in);
        i = decoder.readLong();
        assertEquals(-1L, i);

        in = new ByteArrayInputStream(new byte[]{
            (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
        decoder = new MemcmpDecoder(in);
        i = decoder.readLong();
        assertEquals(0L, i);
    }

    @Test
    public void testReadBytes() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[]{
            (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0xff, (byte) 0x00, (byte) 0x00});
        Decoder decoder = new MemcmpDecoder(in);
        ByteBuffer bytes = decoder.readBytes(null);
        assertArrayEquals(new byte[]{(byte) 0x01, (byte) 0x00, (byte) 0xff}, bytes.array());
    }

    @Test
    public void testReadEncoderOutput() throws Exception {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        Encoder encoder = new MemcmpEncoder(byteOutputStream);
        encoder.writeFloat(1.1f);
        InputStream in = new ByteArrayInputStream(byteOutputStream.toByteArray());
        Decoder decoder = new MemcmpDecoder(in);
        float readFloat = decoder.readFloat();
        assertEquals(1.1f, readFloat, 0.0001);

        byteOutputStream = new ByteArrayOutputStream();
        encoder = new MemcmpEncoder(byteOutputStream);
        encoder.writeDouble(1.1d);
        in = new ByteArrayInputStream(byteOutputStream.toByteArray());
        decoder = new MemcmpDecoder(in);
        double readDouble = decoder.readDouble();
        assertEquals(1.1d, readDouble, 0.0001);

        byteOutputStream = new ByteArrayOutputStream();
        encoder = new MemcmpEncoder(byteOutputStream);
        encoder.writeString("hello there");
        in = new ByteArrayInputStream(byteOutputStream.toByteArray());
        decoder = new MemcmpDecoder(in);
        Utf8 readString = decoder.readString(null);
        assertEquals("hello there", readString.toString());
    }

    @Test
    public void testRecordEncodedOutput() throws Exception {
        Schema schema = new Schema.Parser().parse("{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\": \"com.brightcove.rna.model\",\n" +
            "  \"name\" : \"DimensionSet\",\n" +
            "  \"fields\" : [\n" +
            "    {\"name\" : \"account\", \"type\" : \"string\"},\n" +
            "    {\"name\" : \"event_time\", \"type\" : \"long\"},\n" +
            "    {\"name\" : \"version\", \"type\" : \"int\"},\n" +
            "    {\"name\" : \"player\", \"type\" : [ \"null\", \"string\" ]},\n" +
            "    {\"name\" : \"video\", \"type\" : [ \"null\", \"string\" ]}\n" +
            "  ]\n" +
            "}");
        byte[] bytes1 = toBytes(new GenericRecordBuilder(schema)
            .set("account", "account-1")
            .set("event_time", 11111120L)
            .set("version", 1)
            .set("player", "p-1")
            .set("video", "v-1")
            .build(), schema);
        byte[] bytes2 = toBytes(new GenericRecordBuilder(schema)
            .set("account", "account-1")
            .set("version", 1)
            .set("event_time", 11111121L)
            .set("player", "p-1")
            .set("video", "v-1")
            .build(), schema);
        byte[] bytes3 = toBytes(new GenericRecordBuilder(schema)
            .set("account", "account-2")
            .set("version", 1)
            .set("player", "p-2")
            .set("event_time", 11111122L)
            .set("video", "v-3")
            .build(), schema);
        List<byte[]> serializedRecords = Lists.newArrayList(bytes1, bytes3, bytes2);

        //assert memcmp sortability
        Collections.sort(serializedRecords, Bytes.BYTES_COMPARATOR);
        assertEquals(bytes1, serializedRecords.get(0));
        assertEquals(bytes2, serializedRecords.get(1));
        assertEquals(bytes3, serializedRecords.get(2));

        byte[] accountBytes = toBytes("account-1");
        // regex to get all rows for the account='account-1'

        Pattern.compile(
            //|---- <account>-----|-<ts>-|------ <version>----|-------<array-element:player>-----|-------<array-element:video>------|
            "(account-1\\x00\\x00)(.{8})(\\x80\\x00\\x00\\x01)(\\x80\\x00\\x00\\x01p-1\\x00\\x00)(\\x80\\x00\\x00\\x01v-1\\x00\\x00)");
    }

    private byte[] toBytes(String str) throws IOException {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        Encoder encoder = new MemcmpEncoder(byteOutputStream);
        encoder.writeString(str);
        encoder.flush();
        byteOutputStream.flush();
        byteOutputStream.close();
        return byteOutputStream.toByteArray();
    }

    private byte[] toBytes(GenericData.Record record, Schema schema) throws IOException {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        Encoder encoder = new MemcmpEncoder(byteOutputStream);
        GenericDatumWriter writer = new GenericDatumWriter(schema);
        writer.write(record, encoder);
        encoder.flush();
        byteOutputStream.flush();
        byteOutputStream.close();
        return byteOutputStream.toByteArray();
    }
}
