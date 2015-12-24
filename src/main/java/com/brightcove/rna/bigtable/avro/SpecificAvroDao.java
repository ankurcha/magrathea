package com.brightcove.rna.bigtable.avro;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;

public class SpecificAvroDao {
    private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

    private static <K extends IndexedRecord, E extends SpecificRecord> AvroEntityMapper<K, E> buildEntityMapper(
        String readerSchemaStr, String writtenSchemaStr, Class<E> entityClass) {
        AvroEntitySchema readerSchema = parser.parseEntitySchema(readerSchemaStr);
        // The specific class may have been compiled with a setting that adds the
        // string type to the string fields, but aren't in the local or managed
        // schemas.
        readerSchema = AvroUtils.mergeSpecificStringTypes(entityClass, readerSchema);
        AvroEntitySchema writtenSchema = parser.parseEntitySchema(writtenSchemaStr);
        AvroEntityComposer<E> entityComposer = new AvroEntityComposer<E>(readerSchema, true);
        AvroEntitySerDe<E> entitySerDe = new AvroEntitySerDe<E>(entityComposer, readerSchema, writtenSchema, true);

        AvroKeySchema keySchema = parser.parseKeySchema(readerSchemaStr);
        keySchema = AvroUtils.mergeSpecificStringTypes(entityClass, keySchema);
        AvroKeySerDe<K> keySerDe = new AvroKeySerDe<>(keySchema.getAvroSchema());

        return new AvroEntityMapper<>(keySchema, readerSchema, keySerDe, entitySerDe);
    }


}

