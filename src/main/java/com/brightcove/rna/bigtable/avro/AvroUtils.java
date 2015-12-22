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
        int bytesRead = 0;
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
        List<Schema.Field> defaultFields = new ArrayList<Schema.Field>();
        for (Schema.Field f : avroRecordSchema.getFields()) {
            if (f.defaultValue() != null) {
                // Need to create a new Field here or we will get
                // org.apache.avro.AvroRuntimeException: Field already used:
                // schemaVersion
                defaultFields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue(), f.order()));
            }
        }

        Schema defaultSchema = Schema.createRecord(defaultFields);
        Schema emptyRecordSchema = Schema.createRecord(new ArrayList<Schema.Field>());
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
            emptyRecordSchema);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
            emptyRecordSchema, defaultSchema);

        GenericRecord emptyRecord = new GenericData.Record(emptyRecordSchema);
        GenericRecord defaultRecord = AvroUtils.readAvroEntity(
            AvroUtils.writeAvroEntity(emptyRecord, writer), reader);

        Map<String, Object> defaultValueMap = new HashMap<String, Object>();
        for (Schema.Field f : defaultFields) {
            defaultValueMap.put(f.name(), defaultRecord.get(f.name()));
        }
        return defaultValueMap;
    }

    /**
     * Returns true if the types of two avro schemas are equal. This ignores
     * things like custom field properties that the equals() implementation of
     * Schema checks.
     *
     * @param schema1 The first schema to compare
     * @param schema2 The second schema to compare
     * @return True if the types are equal, otherwise false.
     */
    public static boolean avroSchemaTypesEqual(Schema schema1, Schema schema2) {
        if (schema1.getType() != schema2.getType()) {
            // if the types aren't equal, no need to go further. Return false
            return false;
        }

        if (schema1.getType() == Schema.Type.ENUM
            || schema1.getType() == Schema.Type.FIXED) {
            // Enum and Fixed types schemas should be equal using the Schema.equals
            // method.
            return schema1.equals(schema2);
        }
        if (schema1.getType() == Schema.Type.ARRAY) {
            // Avro element schemas should be equal, which is tested by recursively
            // calling this method.
            return avroSchemaTypesEqual(schema1.getElementType(),
                schema2.getElementType());
        } else if (schema1.getType() == Schema.Type.MAP) {
            // Map type values schemas should be equal, which is tested by recursively
            // calling this method.
            return avroSchemaTypesEqual(schema1.getValueType(),
                schema2.getValueType());
        } else if (schema1.getType() == Schema.Type.UNION) {
            // Compare Union fields in the same position by comparing their schemas
            // recursively calling this method.
            if (schema1.getTypes().size() != schema2.getTypes().size()) {
                return false;
            }
            for (int i = 0; i < schema1.getTypes().size(); i++) {
                if (!avroSchemaTypesEqual(schema1.getTypes().get(i), schema2.getTypes()
                    .get(i))) {
                    return false;
                }
            }
            return true;
        } else if (schema1.getType() == Schema.Type.RECORD) {
            // Compare record fields that match in name by comparing their schemas
            // recursively calling this method.
            for (Schema.Field field1 : schema1.getFields()) {
                Schema.Field field2 = schema2.getField(field1.name());
                if (field2 == null) {
                    return false;
                }
                if (!avroSchemaTypesEqual(field1.schema(), field2.schema())) {
                    return false;
                }
            }
            return true;
        } else {
            // All other types are primitive, so them matching in type is enough.
            return true;
        }
    }

}
