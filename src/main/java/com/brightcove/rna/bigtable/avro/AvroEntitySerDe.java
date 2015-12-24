package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.EntitySerDe;
import com.brightcove.rna.bigtable.avro.io.ColumnDecoder;
import com.brightcove.rna.bigtable.avro.io.ColumnEncoder;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.MappingType;
import com.google.bigtable.repackaged.com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.brightcove.rna.bigtable.core.MappingType.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.avro.Schema.Type.*;

public class AvroEntitySerDe<E extends IndexedRecord> implements EntitySerDe<E> {

    /**
     * Set of mapping types that can be handled
     */
    private static final ImmutableSet<MappingType> VALID_MAPPING_TYPES = ImmutableSet.of(COLUMN, COUNTER, KEY_AS_COLUMN);

    private final AvroEntityComposer<E> entityComposer;

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

    /**
     * Constructor for AvroEntitySerDe instances.
     *
     * @param entityComposer    An entity composer that can construct Avro entities
     * @param avroSchema        The avro schema for entities this SerDe serializes and deserializes
     * @param writtenAvroSchema The avro schema a record we are reading was written with
     * @param specific          True if the entity is a Specific avro record. False indicates it's a generic
     */
    public AvroEntitySerDe(AvroEntityComposer<E> entityComposer, AvroEntitySchema avroSchema, AvroEntitySchema writtenAvroSchema, boolean specific) {
        this.entityComposer = entityComposer;
        this.specific = specific;
        this.avroSchema = avroSchema;

        // For each field in entity, initialize the appropriate datum readers and writers.
        for (FieldMapping fieldMapping : avroSchema.getFieldMappings()) {
            String fieldName = fieldMapping.fieldName();
            Schema fieldSchema = avroSchema.getAvroSchema().getField(fieldName).schema();
            Schema.Field writtenField = writtenAvroSchema.getAvroSchema().getField(fieldName);
            // No field for the written version, so don't worry about datum readers and writers.
            if (writtenField == null) {
                continue;
            }
            Schema writtenFieldSchema = writtenField.schema();
            MappingType mappingType = fieldMapping.mappingType();

            if (mappingType == MappingType.COLUMN || mappingType == MappingType.COUNTER) {
                initColumnDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
            }

            if (mappingType == MappingType.KEY_AS_COLUMN) {
                Schema.Type type = fieldSchema.getType();
                String value = fieldMapping.mappingValue();
                checkArgument(type == RECORD || type == MAP, "Unsupported type for keyAsColumn: %s", value);
                if (type == RECORD) {
                    // Each field of the kac record has a different type, so we need to track each one in a different map.
                    initKACRecordDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
                } else if (type == MAP) {
                    // Only one value type for a map, so just put the type in the column datum maps.
                    initColumnDatumMaps(fieldName, fieldSchema.getValueType(), writtenFieldSchema.getValueType());
                }

            }
        }
    }

    /**
     * Serialize an entity's field value to a PutAction.
     *
     * @param keyBytes   The bytes of the serialized key (needed to construct a Put).
     * @param mapping    The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue The value of the field to serialize.
     * @return The PutAction with column's populated with the field's serialized values.
     */
    @Override
    public Put serialize(byte[] keyBytes, FieldMapping mapping, Object fieldValue) {
        Put put = new Put(keyBytes);
        String fieldName = mapping.fieldName();
        MappingType type = mapping.mappingType();

        checkArgument(VALID_MAPPING_TYPES.contains(type), "Invalid field mapping for field with name: %s", fieldName);

        if (type == MappingType.COLUMN || type == MappingType.COUNTER) {
            serializeColumn(mapping, fieldValue, put);
        } else if (type == MappingType.KEY_AS_COLUMN) {
            serializeKeyAsColumn(mapping, fieldValue, put);
        }

        return put;
    }

    /**
     * Deserialize an entity field from the HBase Result.
     *
     * @param mapping The FieldMapping that specifies this field's mapping type and field name.
     * @param result  The HBase Result that represents a row in HBase.
     * @return The field Object we deserialized from the Result.
     */
    @Override
    public Object deserialize(FieldMapping mapping, Result result) {
        String fieldName = mapping.fieldName();
        MappingType mappingType = mapping.mappingType();

        // validate
        ImmutableList<MappingType> validTypes = ImmutableList.of(COLUMN, COUNTER, KEY_AS_COLUMN);
        checkArgument(validTypes.contains(mappingType), "Invalid field mapping for field with name: %s", fieldName);

        if (mappingType == MappingType.COLUMN || mappingType == MappingType.COUNTER) {
            return deserializeColumn(mapping, result);
        } else if (mappingType == MappingType.KEY_AS_COLUMN) {
            return deserializeKeyAsColumn(mapping, result);
        }
        return null;
    }

    /**
     * Serialize the column value, and update the Put with the serialized bytes.
     *
     * @param mapping    The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue The value we are serializing
     * @param put        The Put we are updating with the serialized bytes.
     */
    private void serializeColumn(FieldMapping mapping, Object fieldValue, Put put) {
        // column mapping, so simply serialize the value and add the bytes to the put.
        checkArgument(mapping.mappingType() == COLUMN || mapping.mappingType() == COUNTER);

        byte[] bytes = serializeColumnValueToBytes(mapping.fieldName(), fieldValue);
        put.addColumn(mapping.family(), mapping.qualifier(), bytes);
    }

    /**
     * Serialize a keyAsColumn field, and update the put with the serialized bytes from each subfield of the keyAsColumn value.
     *
     * @param mapping    The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue The value we are serializing
     * @param put        The put to update with the serialized bytes.
     */
    private void serializeKeyAsColumn(FieldMapping mapping, Object fieldValue, Put put) {
        // keyAsColumn mapping, so extract each value from the keyAsColumn field
        // using the entityComposer, serialize them, and them to the put.
        checkArgument(mapping.mappingType() == KEY_AS_COLUMN);
        String fieldName = mapping.fieldName();
        Map<CharSequence, Object> keyAsColumnValues = entityComposer.extractKeyAsColumnValues(fieldName, fieldValue);
        for (Map.Entry<CharSequence, Object> entry : keyAsColumnValues.entrySet()) {
            CharSequence qualifier = entry.getKey();
            byte[] qualifierBytes = serializeKeyAsColumnKeyToBytes(fieldName, qualifier);
            // serialize the value, and add it to the put.
            byte[] bytes = serializeKeyAsColumnValueToBytes(fieldName, qualifier, entry.getValue());
            put.addColumn(mapping.family(), qualifierBytes, bytes);
        }
    }

    /**
     * Deserialize the entity field that has a column mapping.
     *
     * @param mapping The FieldMapping that specifies this field's mapping type and field name.
     * @param result  The HBase Result that represents a row in HBase.
     * @return The deserialized field value
     */
    private Object deserializeColumn(FieldMapping mapping, Result result) {
        MappingType mappingType = mapping.mappingType();
        checkArgument(mappingType == MappingType.COLUMN || mappingType == MappingType.COUNTER);
        byte[] bytes = result.getValue(mapping.family(), mapping.qualifier());
        return bytes == null ? null : deserializeColumnValueFromBytes(mapping.fieldName(), bytes);
    }

    /**
     * Deserialize the entity field that has a keyAsColumn mapping.
     *
     * @param mapping The FieldMapping that specifies this field's mapping type and field name.
     * @param result  The HBase Result that represents a row in HBase.
     * @return The deserialized entity field value.
     */
    private Object deserializeKeyAsColumn(FieldMapping mapping, Result result) {
        checkArgument(mapping.mappingType() == KEY_AS_COLUMN);
        // Construct a map of keyAsColumn field values. From this we'll be able
        // to use the entityComposer to construct the entity field value.
        Map<CharSequence, Object> fieldValueAsMap = Maps.newHashMap();
        Map<byte[], byte[]> familyMap = result.getFamilyMap(mapping.family());
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] columnBytes = entry.getValue();
            CharSequence keyAsColumnKey = deserializeKeyAsColumnKeyFromBytes(mapping.fieldName(), qualifier);
            Object keyAsColumnValue = deserializeKeyAsColumnValueFromBytes(mapping.fieldName(), qualifier, columnBytes);
            fieldValueAsMap.put(keyAsColumnKey, keyAsColumnValue);
        }
        // Now build the entity field from the fieldValueAsMap.
        return entityComposer.buildKeyAsColumnField(mapping.fieldName(), fieldValueAsMap);
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

    public Object deserializeColumnValueFromBytes(String fieldName, byte[] columnBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);

        checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        checkNotNull(datumReader, "No datum reader for field name: %s", fieldName);

        ByteArrayInputStream byteIn = new ByteArrayInputStream(columnBytes);
        Decoder decoder = getColumnDecoder(field.schema(), byteIn);

        return AvroUtils.readAvroEntity(decoder, datumReader);
    }

    public Object deserializeKeyAsColumnValueFromBytes(String fieldName, byte[] columnKeyBytes, byte[] columnValueBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        Schema.Type schemaType = field.schema().getType();

        checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        checkArgument(schemaType == MAP || schemaType == RECORD, "Unsupported type for keyAsColumn: %s", schemaType);

        switch (schemaType) {
            case MAP: {
                DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);
                checkNotNull(datumReader, "No datum reader for field name: %s", fieldName);

                return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
            }
            case RECORD: {
                String columnKey = new String(columnKeyBytes);

                checkArgument(kacRecordDatumReaders.containsKey(fieldName),
                    "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
                checkArgument(kacRecordDatumReaders.get(fieldName).containsKey(columnKey),
                    "Invalid key in record: %s.%s", fieldName, columnKey);

                DatumReader<Object> datumReader = kacRecordDatumReaders.get(fieldName).get(columnKey);
                return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
            }
        }
        return null;
    }

    public CharSequence deserializeKeyAsColumnKeyFromBytes(String fieldName, byte[] columnKeyBytes) {
        Schema.Field field = avroSchema.getAvroSchema().getField(fieldName);
        Schema.Type schemaType = field.schema().getType();

        Preconditions.checkNotNull(field, "Invalid field name %s for schema %s", fieldName, avroSchema.toString());
        checkArgument(schemaType == MAP || schemaType == RECORD, "Unsupported type for keyAsColumn: %s", schemaType);
        switch (schemaType) {
            case RECORD:
                return new String(columnKeyBytes);
            case MAP:
                String stringProp = field.schema().getProp("avro.java.string");
                return (stringProp != null && stringProp.equals("String")) ? new String(columnKeyBytes) : new Utf8(columnKeyBytes);
        }
        return null;
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
        Schema.Type type = fieldAvroSchema.getType();
        if (type == INT || type == LONG || type == STRING) {
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
        Schema.Type type = writtenFieldAvroSchema.getType();
        if (type == INT || type == LONG || type == STRING) {
            return new ColumnDecoder(in);
        } else {
            return DecoderFactory.get().binaryDecoder(in, null);
        }
    }
}
