package com.brightcove.rna.bigtable.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.lang.SerializationException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;

public class AvroUtils {

    /**
     * Given a byte array and a DatumReader, decode an avro entity from the byte array. Decodes using the avro
     * BinaryDecoder. Return the constructed entity.
     *
     * @param bytes  The byte array to decode the entity from.
     * @param reader The DatumReader that will decode the byte array.
     * @return The Avro entity.
     */
    public static <T> T readAvroEntity(byte[] bytes, DatumReader<T> reader) {
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return AvroUtils.readAvroEntity(decoder, reader);
    }

    /**
     * Decode an entity from the initialized Avro Decoder using the DatumReader.
     *
     * @param decoder The decoder to decode the entity fields
     * @param reader  The Avro DatumReader that will read the entity with the decoder.
     * @return The entity.
     */
    public static <T> T readAvroEntity(Decoder decoder, DatumReader<T> reader) {
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Could not deserialize Avro entity", e);
        }
    }

    /**
     * Given an entity and a DatumReader, encode the avro entity to a byte array.
     * Encodes using the avro BinaryEncoder. Return the serialized bytes.
     *
     * @param entity The entity we want to encode.
     * @param writer The DatumWriter we'll use to encode the entity to a byte array
     * @return The avro entity encoded in a byte array.
     */
    public static <T> byte[] writeAvroEntity(T entity, DatumWriter<T> writer) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new EncoderFactory().binaryEncoder(outputStream, null);
        writeAvroEntity(entity, encoder, writer);
        return outputStream.toByteArray();
    }

    /**
     * Given an entity, an avro schema, and an encoder, write the entity to the encoder's underlying output stream.
     *
     * @param entity  The entity we want to encode.
     * @param encoder The Avro Encoder we will write to.
     * @param writer  The DatumWriter we'll use to encode the entity to the encoder.
     */
    public static <T> void writeAvroEntity(T entity, Encoder encoder, DatumWriter<T> writer) {
        try {
            writer.write(entity, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new SerializationException("Could not serialize Avro entity", e);
        }
    }

    /**
     * Given an avro Schema.Field instance, make a clone of it.
     *
     * @param field The field to clone.
     * @return The cloned field.
     */
    public static Schema.Field cloneField(Schema.Field field) {
        return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
    }

    /**
     * Convert an InputStream to a string encoded as UTF-8.
     *
     * @param in The InputStream to read the schema from.
     * @return The string.
     */
    public static String inputStreamToString(InputStream in) throws IOException {
        final int BUFFER_SIZE = 1024;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, UTF_8));
        char[] buffer = new char[BUFFER_SIZE];
        StringBuilder stringBuilder = new StringBuilder(BUFFER_SIZE);
        int bytesRead;
        while ((bytesRead = bufferedReader.read(buffer, 0, BUFFER_SIZE)) > 0) {
            stringBuilder.append(buffer, 0, bytesRead);
        }
        return stringBuilder.toString();
    }

    /**
     * Get a map of field names to default values for an Avro schema.
     *
     * @param avroRecordSchema The schema to get the map of field names to values.
     * @return The map.
     */
    public static Map<String, Object> getDefaultValueMap(Schema avroRecordSchema) {
        List<Schema.Field> defaultFields = avroRecordSchema.getFields().stream()
            .filter(f -> f.defaultValue() != null)
            .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue(), f.order()))
            .collect(Collectors.toList());

        Schema defaultSchema = Schema.createRecord(defaultFields);
        Schema emptyRecordSchema = Schema.createRecord(new ArrayList<>());
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(emptyRecordSchema);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(emptyRecordSchema, defaultSchema);

        GenericRecord emptyRecord = new GenericData.Record(emptyRecordSchema);
        GenericRecord defaultRecord = AvroUtils.readAvroEntity(AvroUtils.writeAvroEntity(emptyRecord, writer), reader);

        Map<String, Object> defaultValueMap = new HashMap<>();
        for (Schema.Field f : defaultFields) {
            defaultValueMap.put(f.name(), defaultRecord.get(f.name()));
        }
        return defaultValueMap;
    }

}
