package com.brightcove.rna.bigtable.avro;

import org.apache.avro.specific.SpecificRecord;

public class AvroDao {
    private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

    public static <E extends SpecificRecord> AvroEntityMapper<E> getEntityMapper(
        String readerSchemaStr, String writtenSchemaStr, Class<E> entityClass) {

        AvroEntitySchema readerSchema = parser.parseEntitySchema(readerSchemaStr);
        readerSchema = AvroUtils.mergeSpecificStringTypes(entityClass, readerSchema);
        AvroEntitySchema writtenSchema = parser.parseEntitySchema(writtenSchemaStr);
        AvroEntityComposer<E> entityComposer = new AvroEntityComposer<>(readerSchema, true);
        AvroEntitySerDe<E> entitySerDe = new AvroEntitySerDe<>(entityComposer, readerSchema, writtenSchema, true);

        AvroKeySchema keySchema = parser.parseKeySchema(readerSchemaStr);
        keySchema = AvroUtils.mergeSpecificStringTypes(entityClass, keySchema);
        AvroKeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema());

        return new AvroEntityMapper<>(keySchema, readerSchema, keySerDe, entitySerDe);
    }


}

