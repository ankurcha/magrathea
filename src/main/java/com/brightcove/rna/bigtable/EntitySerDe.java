package com.brightcove.rna.bigtable;

import com.brightcove.rna.bigtable.avro.AvroEntityComposer;
import com.brightcove.rna.bigtable.core.FieldMapping;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface EntitySerDe<E extends IndexedRecord> {
    /**
     * Serialize an entity's field value to a Put.
     *
     * @param keyBytes     The bytes of the serialized key (needed to construct a Put).
     * @param fieldMapping The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue   The value of the field to serialize.
     * @return The Put with column's populated with the field's serialized values.
     */
    Put serialize(byte[] keyBytes, FieldMapping fieldMapping, Object fieldValue);

    /**
     * Serialize an entity's field value to a given put.
     * @param oldPut Existing Put instance that should up updated
     * @param fieldMapping The FieldMapping that specifies this field's mapping type and field name.
     * @param fieldValue The value of the field to serialize.
     * @return The Put with column's populated with the field's serialized values.
     */
    Put serialize(Put oldPut, FieldMapping fieldMapping, Object fieldValue);

    /**
     * Deserialize an entity field from the HBase Result.
     *
     * @param fieldMapping The FieldMapping that specifies this field's mapping type and field name.
     * @param result       The HBase Result that represents a row in HBase.
     * @return The field Object we deserialized from the Result.
     */
    Object deserialize(FieldMapping fieldMapping, Result result);


    /**
     * Get the EntityComposer this EntitySerDe uses to compose entity fields.
     *
     * @return The EntityComposer
     */
    AvroEntityComposer<E> getEntityComposer();
}
