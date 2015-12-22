package com.brightcove.rna.bigtable;

import com.brightcove.rna.bigtable.avro.AvroEntityComposer;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.MappingType;
import com.brightcove.rna.bigtable.core.SchemaValidationException;
import com.google.common.collect.Maps;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

public abstract class EntitySerDe<E extends IndexedRecord> {
    private final AvroEntityComposer<E> entityComposer;

    public EntitySerDe(AvroEntityComposer<E> entityComposer) {
        this.entityComposer = entityComposer;
    }

    /**
     * Serialize an entity's field value to a PutAction.
     *
     * @param keyBytes     The bytes of the serialized key (needed to construct a PutAction).
     * @param fieldMapping The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue   The value of the field to serialize.
     * @return The PutAction with column's populated with the field's serialized values.
     */
    public Put serialize(byte[] keyBytes, FieldMapping fieldMapping, Object fieldValue) {
        Put put = new Put(keyBytes);
        String fieldName = fieldMapping.fieldName();
        switch (fieldMapping.mappingType()) {
            case COLUMN:
            case COUNTER:
                serializeColumn(fieldName, fieldMapping.family(), fieldMapping.qualifier(), fieldValue, put);
                break;
            case KEY_AS_COLUMN:
                serializeKeyAsColumn(fieldName, fieldMapping.family(), fieldValue, put);
                break;
            default:
                throw new SchemaValidationException("Invalid field mapping for field with name: " + fieldMapping.fieldName());
        }
        return put;
    }

    /**
     * Deserialize an entity field from the HBase Result.
     *
     * @param fieldMapping The FieldMapping that specifies this field's mapping type and field name.
     * @param result       The HBase Result that represents a row in HBase.
     * @return The field Object we deserialized from the Result.
     */
    public Object deserialize(FieldMapping fieldMapping, Result result) {
        String fieldName = fieldMapping.fieldName();
        MappingType mappingType = fieldMapping.mappingType();
        if (mappingType == MappingType.COLUMN || mappingType == MappingType.COUNTER) {
            return deserializeColumn(fieldMapping.fieldName(), fieldMapping.family(), fieldMapping.qualifier(), result);
        } else if (mappingType == MappingType.KEY_AS_COLUMN) {
            return deserializeKeyAsColumn(fieldMapping.fieldName(), fieldMapping.family(), result);
        } else {
            throw new SchemaValidationException(String.format("Invalid field mapping for field with name: %s", fieldName));
        }
    }

    /**
     * Serialize the column mapped entity field value to bytes.
     *
     * @param fieldName  The name of the entity's field
     * @param fieldValue The value to serialize
     * @return The serialized bytes
     */
    public abstract byte[] serializeColumnValueToBytes(String fieldName, Object fieldValue);

    /**
     * Serialize a value from a keyAsColumn entity field. The value is keyed on
     * the key.
     *
     * @param fieldName             The name of the entity's keyAsColumn field
     * @param columnKey             The key of the keyAsColumn field
     * @param keyAsColumnFieldValue The value pointed to by this key.
     * @return The serialized bytes
     */
    public abstract byte[] serializeKeyAsColumnValueToBytes(String fieldName, CharSequence columnKey, Object keyAsColumnFieldValue);

    /**
     * Serialize the keyAsColumn key to bytes.
     *
     * @param fieldName The name of the entity's keyAsColumn field
     * @param columnKey The column key to serialize to bytes
     * @return The serialized bytes.
     */
    public abstract byte[] serializeKeyAsColumnKeyToBytes(String fieldName, CharSequence columnKey);

    /**
     * Deserialize a column mapped entity field's bytes to its type.
     *
     * @param fieldName   The name of the entity's field
     * @param columnBytes The bytes to deserialize
     * @return The field value we've deserialized.
     */
    public abstract Object deserializeColumnValueFromBytes(String fieldName, byte[] columnBytes);

    /**
     * Deserialize a value from a keyAsColumn entity field. The value is keyed on
     * key.
     *
     * @param fieldName        The name of the entity's keyAsColumn field
     * @param columnKeyBytes   The key bytes of the keyAsColumn field
     * @param columnValueBytes The value bytes to deserialize
     * @return The keyAsColumn value pointed to by key.
     */
    public abstract Object deserializeKeyAsColumnValueFromBytes(String fieldName, byte[] columnKeyBytes, byte[] columnValueBytes);

    /**
     * Deserialize the keyAsColumn key from the qualifier.
     *
     * @param fieldName      The name of the keyAsColumn field
     * @param columnKeyBytes The bytes of the qualifier
     * @return The deserialized CharSequence
     */
    public abstract CharSequence deserializeKeyAsColumnKeyFromBytes(String fieldName, byte[] columnKeyBytes);


    /**
     * Serialize the column value, and update the Put with the serialized bytes.
     *
     * @param fieldName  The name of the entity field we are serializing
     * @param family     The column family this field maps to
     * @param qualifier  The qualifier this field maps to
     * @param fieldValue The value we are serializing
     * @param put        The Put we are updating with the serialized bytes.
     */
    private void serializeColumn(String fieldName, byte[] family, byte[] qualifier, Object fieldValue, Put put) {
        // column mapping, so simply serialize the value and add the bytes to the put.
        byte[] bytes = serializeColumnValueToBytes(fieldName, fieldValue);
        put.addColumn(family, qualifier, bytes);
    }

    /**
     * Serialize a keyAsColumn field, and update the put with the serialized bytes from each subfield of the keyAsColumn value.
     *
     * @param fieldName  The name of the entity field we are serializing
     * @param family     The column family this field maps to
     * @param fieldValue The value we are serializing
     * @param put        The put to update with the serialized bytes.
     */
    private void serializeKeyAsColumn(String fieldName, byte[] family, Object fieldValue, Put put) {
        // keyAsColumn mapping, so extract each value from the keyAsColumn field
        // using the entityComposer, serialize them, and them to the put.
        Map<CharSequence, Object> keyAsColumnValues = entityComposer.extractKeyAsColumnValues(fieldName, fieldValue);
        for (Map.Entry<CharSequence, Object> entry : keyAsColumnValues.entrySet()) {
            CharSequence qualifier = entry.getKey();
            byte[] qualifierBytes = serializeKeyAsColumnKeyToBytes(fieldName, qualifier);
            // serialize the value, and add it to the put.
            byte[] bytes = serializeKeyAsColumnValueToBytes(fieldName, qualifier, entry.getValue());
            put.addColumn(family, qualifierBytes, bytes);
        }
    }

    /**
     * Deserialize the entity field that has a column mapping.
     *
     * @param fieldName The name of the entity's field we are deserializing.
     * @param family    The column family this field is mapped to
     * @param qualifier The column qualifier this field is mapped to
     * @param result    The HBase Result that represents a row in HBase.
     * @return The deserialized field value
     */
    private Object deserializeColumn(String fieldName, byte[] family, byte[] qualifier, Result result) {
        byte[] bytes = result.getValue(family, qualifier);
        return bytes == null ? null : deserializeColumnValueFromBytes(fieldName, bytes);
    }

    /**
     * Deserialize the entity field that has a keyAsColumn mapping.
     *
     * @param fieldName The name of the entity's field we are deserializing.
     * @param family    The column family this field is mapped to
     * @param result    The HBase Result that represents a row in HBase.
     * @return The deserialized entity field value.
     */
    private Object deserializeKeyAsColumn(String fieldName, byte[] family, Result result) {
        // Construct a map of keyAsColumn field values. From this we'll be able
        // to use the entityComposer to construct the entity field value.
        Map<CharSequence, Object> fieldValueAsMap = Maps.newHashMap();
        Map<byte[], byte[]> familyMap = result.getFamilyMap(family);
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] columnBytes = entry.getValue();
            CharSequence keyAsColumnKey = deserializeKeyAsColumnKeyFromBytes(fieldName, qualifier);
            Object keyAsColumnValue = deserializeKeyAsColumnValueFromBytes(fieldName, qualifier, columnBytes);
            fieldValueAsMap.put(keyAsColumnKey, keyAsColumnValue);
        }
        // Now build the entity field from the fieldValueAsMap.
        return entityComposer.buildKeyAsColumnField(fieldName, fieldValueAsMap);
    }
}
