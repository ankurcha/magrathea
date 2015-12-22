package com.brightcove.rna.bigtable.avro.io;

import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;

public class ColumnEncoderTest {

  private ByteArrayOutputStream byteOutputStream;
  private Encoder encoder;

  @Before
  public void setUp() {
    byteOutputStream = new ByteArrayOutputStream();
    encoder = new ColumnEncoder(byteOutputStream);
  }

  @Test
  public void testEncodeInt() throws Exception {
    encoder.writeInt(1);
    assertArrayEquals(new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x01}, byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeInt(-1);
    assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff,
        (byte) 0xff }, byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeInt(0);
    assertArrayEquals(new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00 }, byteOutputStream.toByteArray());
  }

  @Test
  public void testEncodeLong() throws Exception {
    encoder.writeLong(1L);
    assertArrayEquals(new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01 },
        byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeLong(-1L);
    assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff,
        (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
        byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeLong(0L);
    assertArrayEquals(new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 },
        byteOutputStream.toByteArray());
  }

  @Test
  public void testWriteBytes() throws Exception {
    byte[] bytes = new byte[] { (byte) 0x01, (byte) 0x00, (byte) 0xff };
    encoder.writeBytes(bytes, 0, 3);
    encoder.flush();
    assertArrayEquals(new byte[]{(byte) 0x06, (byte) 0x01, (byte) 0x00,
        (byte) 0xff}, byteOutputStream.toByteArray());
  }

  @Test
  public void testWriteString() throws Exception {
    String s = "hello";
    encoder.writeString(s);
    encoder.flush();
    assertArrayEquals(s.getBytes("UTF-8"), byteOutputStream.toByteArray());

    byteOutputStream.reset();
    encoder.writeString(new Utf8(s));
    encoder.flush();
    assertArrayEquals(s.getBytes("UTF-8"), byteOutputStream.toByteArray());
  }

}
