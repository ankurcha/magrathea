package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.KeySerDe;
import com.brightcove.rna.bigtable.avro.io.MemcmpDecoder;
import com.brightcove.rna.bigtable.avro.io.MemcmpEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AvroKeySerDe implements KeySerDe {
    private final Schema schema;

    public AvroKeySerDe(Schema schema) {
        this.schema = schema;
    }


    @Override
    public byte[] serialize(IndexedRecord entity) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new MemcmpEncoder(outputStream);
        Schema schemaToUse = schema;
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        GenericRecord record = new GenericData.Record(schemaToUse);
        for (Schema.Field field : schema.getFields()) {
            int pos = entity.getSchema().getField(field.name()).pos();
            record.put(field.pos(), entity.get(pos));
        }
        AvroUtils.writeAvroEntity(record, encoder, datumWriter);
        return outputStream.toByteArray();
    }

    @Override
    public IndexedRecord deserialize(byte[] keyBytes) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(keyBytes);
        Decoder decoder = new MemcmpDecoder(inputStream);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        return AvroUtils.readAvroEntity(decoder, datumReader);
    }
}
