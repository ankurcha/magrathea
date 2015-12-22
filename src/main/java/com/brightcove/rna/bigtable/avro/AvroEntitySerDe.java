package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.EntitySerDe;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.SchemaValidationException;
import com.brightcove.rna.bigtable.avro.io.ColumnDecoder;
import com.brightcove.rna.bigtable.avro.io.ColumnEncoder;
import com.google.bigtable.repackaged.com.google.api.client.repackaged.com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AvroEntitySerDe<E extends IndexedRecord> extends EntitySerDe<E> {
    /**
     * Boolean to indicate whether this is a specific record or generic record SerDe.
     */
    private boolean specific;

    /**
     * The Avro schema for the Avro records this SerDe will serialize and deserialize.
     */
    private AvroEntitySchema avroSchema;

    /**
     * A mapping of Avro entity fields to their DatumReaders
     */
    private Map<String, DatumReader<Object>> fieldDatumReaders = new HashMap<>();

    /**
     * A mapping of Avro entity field names to their DatumWriters
     */
    private Map<String, DatumWriter<Object>> fieldDatumWriters = new HashMap<>();


    /**
     * DatumReaders for keyAsColumn Avro Record fields. The inner map maps from
     * the keyAsColumn Record's fields to each DatumReader. The outer map maps
     * from the Avro entity's field to the inner map.
     */
    private final Map<String, Map<String, DatumReader<Object>>> kacRecordDatumReaders = new HashMap<>();

    /**
     * DatumWriters for keyAsColumn Avro Record fields. The inner map maps from
     * the keyAsColumn Record's fields to each DatumWriter. The outer map maps
     * from the Avro entity's field to the inner map.
     */
    private final Map<String, Map<String, DatumWriter<Object>>> kacRecordDatumWriters = new HashMap<>();

    public AvroEntitySerDe(AvroEntityComposer<E> entityComposer, AvroEntitySchema avroSchema, AvroEntitySchema writtenAvroSchema, boolean specific) {
        super(entityComposer);
        this.specific = specific;
        this.avroSchema = avroSchema;

        // For each field in entity, initialize the appropriate datum readers and writers.
        for (FieldMapping fieldMapping : avroSchema.getFieldMappings()) {
            String fieldName            = fieldMapping.fieldName();
            Schema fieldSchema          = avroSchema.getAvroSchema().getField(fieldName).schema();
            Schema.Field writtenField   = writtenAvroSchema.getAvroSchema().getField(fieldName);
            // No field for the written version, so don't worry about datum readers and writers.
            if (writtenField == null) {
                continue;
            }
            Schema writtenFieldSchema = writtenField.schema();
            switch (fieldMapping.mappingType()) {
                case KEY:
                    break;
                case COLUMN:
                case COUNTER:
                    initColumnDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
                    break;
                case KEY_AS_COLUMN:
                    if (fieldSchema.getType() == Schema.Type.RECORD) {
                        // Each field of the kac record has a different type, so we need to track each one in a different map.
                        initKACRecordDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
                    } else if (fieldSchema.getType() == Schema.Type.MAP) {
                        // Only one value type for a map, so just put the type in the column datum maps.
                        initColumnDatumMaps(fieldName, fieldSchema.getValueType(), writtenFieldSchema.getValueType());
                    } else {
                        throw new IllegalArgumentException("Unsupported type for keyAsColumn: " + fieldMapping.mappingValue());
                    }
                    break;
            }
        }
    }

    private void initColumnDatumMaps(String fieldName, Schema fieldSchema, Schema writtenFieldSchema) {
        fieldDatumReaders.put(fieldName, buildDatumReader(fieldSchema, writtenFieldSchema));
        fieldDatumWriters.put(fieldName, buildDatumWriter(fieldSchema));
    }

    private void initKACRecordDatumMaps(String fieldName, Schema fieldSchema, Schema writtenFieldSchema) {
        Map<String, DatumReader<Object>> recordFieldReaderMap = new HashMap<>();
        Map<String, DatumWriter<Object>> recordFieldWriterMap = new HashMap<>();
        kacRecordDatumReaders.put(fieldName, recordFieldReaderMap);
        kacRecordDatumWriters.put(fieldName, recordFieldWriterMap);
        for (Schema.Field recordField : fieldSchema.getFields()) {
            Schema.Field writtenRecordField = writtenFieldSchema.getField(recordField.name());
            if (writtenRecordField == null) {
                continue;
            }
            recordFieldReaderMap.put(recordField.name(), buildDatumReader(recordField.schema(), writtenRecordField.schema()));
            recordFieldWriterMap.put(recordField.name(), buildDatumWriter(recordField.schema()));
        }
    }


    private DatumReader<Object> buildDatumReader(Schema schema, Schema writtenSchema) {
        return this.specific ? new SpecificDatumReader<>(writtenSchema, schema) : new GenericDatumReader<>(writtenSchema, schema);
    }

    private DatumWriter<Object> buildDatumWriter(Schema schema) {
        return this.specific ? new SpecificDatumWriter<>(schema) : new GenericDatumWriter<>(schema);
    }


    public byte[] serializeColumnValueToBytes(String fieldName, Object columnValue) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        DatumWriter<Object> datumWriter = fieldDatumWriters.get(fieldName);

        checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        checkNotNull(datumWriter, "No datum writer for field name: %s", fieldName);

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        Encoder encoder = getColumnEncoder(field.schema(), byteOut);
        AvroUtils.writeAvroEntity(columnValue, encoder, fieldDatumWriters.get(fieldName));
        return byteOut.toByteArray();
    }

    public byte[] serializeKeyAsColumnValueToBytes(String fieldName, CharSequence columnKey, Object columnValue) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        checkArgument(field != null, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        Schema.Type schemaType = field.schema().getType();
        switch (schemaType) {
            case MAP: {
                DatumWriter<Object> datumWriter = fieldDatumWriters.get(fieldName);
                checkArgument(datumWriter != null, "No datum writer for field name: %s", fieldName);
                return AvroUtils.writeAvroEntity(columnValue, datumWriter);
            }
            case RECORD: {
                checkArgument(kacRecordDatumWriters.containsKey(fieldName), "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
                checkArgument(kacRecordDatumWriters.get(fieldName).containsKey(columnKey.toString()), "Invalid key in record: %s.%s", fieldName, columnKey);
                DatumWriter<Object> datumWriter = kacRecordDatumWriters.get(fieldName).get(columnKey.toString());
                return AvroUtils.writeAvroEntity(columnValue, datumWriter);
            }
            default:
                throw new IllegalArgumentException("Unsupported type for keyAsColumn: " + schemaType);
        }
    }

    public byte[] serializeKeyAsColumnKeyToBytes(String fieldName, CharSequence columnKey) {
        if (columnKey.getClass().isAssignableFrom(String.class)) {
            return ((String) columnKey).getBytes();
        } else if (columnKey.getClass().isAssignableFrom(Utf8.class)) {
            return ((Utf8) columnKey).getBytes();
        } else {
            return columnKey.toString().getBytes();
        }
    }

    @Override
    public Object deserializeColumnValueFromBytes(String fieldName, byte[] columnBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);

        checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        checkNotNull(datumReader, "No datum reader for field name: %s", fieldName);

        ByteArrayInputStream byteIn = new ByteArrayInputStream(columnBytes);
        Decoder decoder = getColumnDecoder(field.schema(), byteIn);

        return AvroUtils.readAvroEntity(decoder, datumReader);
    }

    @Override
    public Object deserializeKeyAsColumnValueFromBytes(String fieldName, byte[] columnKeyBytes, byte[] columnValueBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());

        Schema.Type schemaType = field.schema().getType();
        switch (schemaType) {
            case MAP: {
                DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);
                checkNotNull(datumReader, "No datum reader for field name: %s", fieldName);
                return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
            }
            case RECORD: {
                checkArgument(kacRecordDatumReaders.containsKey(fieldName), "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
                String columnKey = new String(columnKeyBytes);
                checkArgument(kacRecordDatumReaders.get(fieldName).containsKey(columnKey), "Invalid key in record: %s.%s", fieldName, columnKey);

                DatumReader<Object> datumReader = kacRecordDatumReaders.get(fieldName).get(columnKey);
                return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
            }
            default:
                throw new SchemaValidationException("Unsupported type for keyAsColumn: " + schemaType);
        }
    }

    @Override
    public CharSequence deserializeKeyAsColumnKeyFromBytes(String fieldName, byte[] columnKeyBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        Preconditions.checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());

        Schema.Type schemaType = field.schema().getType();
        switch (schemaType) {
            case RECORD:
                return new String(columnKeyBytes);
            case MAP:
                String stringProp = field.schema().getProp("avro.java.string");
                return (stringProp != null && stringProp.equals("String")) ? new String(columnKeyBytes) : new Utf8(columnKeyBytes);
            default:
                throw new SchemaValidationException("Unsupported type for keyAsColumn: " + schemaType);
        }
    }

    /**
     * Returns an Avro Encoder. The implementation it chooses will depend on the schema of the field.
     *
     * @param out Output stream to encode bytes to
     * @return The avro encoder
     */
    private Encoder getColumnEncoder(Schema fieldAvroSchema, OutputStream out) {
        // Use a special Avro encoder that has special handling for int, long, and String types.
        // See ColumnEncoder for more information.
        if (fieldAvroSchema.getType() == Schema.Type.INT ||
            fieldAvroSchema.getType() == Schema.Type.LONG ||
            fieldAvroSchema.getType() == Schema.Type.STRING) {
            return new ColumnEncoder(out);
        } else {
            return EncoderFactory.get().binaryEncoder(out, null);
        }
    }


    /**
     * Returns an Avro Decoder. The implementation it chooses will depend on the schema of the field.
     *
     * @param in InputStream to decode bytes from
     * @return The avro decoder.
     */
    private Decoder getColumnDecoder(Schema writtenFieldAvroSchema, InputStream in) {
        // Use a special Avro decoder that has special handling for int, long, and String types.
        // See ColumnDecoder for more information.
        if (writtenFieldAvroSchema.getType() == Schema.Type.INT ||
            writtenFieldAvroSchema.getType() == Schema.Type.LONG ||
            writtenFieldAvroSchema.getType() == Schema.Type.STRING) {
            return new ColumnDecoder(in);
        } else {
            return DecoderFactory.get().binaryDecoder(in, null);
        }
    }
}
