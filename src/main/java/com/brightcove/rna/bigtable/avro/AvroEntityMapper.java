package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.EntityMapper;
import com.brightcove.rna.bigtable.EntitySerDe;
import com.brightcove.rna.bigtable.KeySerDe;
import com.brightcove.rna.bigtable.core.EntitySchema;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.KeySchema;
import com.brightcove.rna.bigtable.core.MappingType;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.Set;

public class AvroEntityMapper<E extends IndexedRecord> implements EntityMapper<E> {
    private final KeySchema keySchema;
    private final EntitySchema entitySchema;
    private final KeySerDe keySerDe;
    private final EntitySerDe<E> entitySerDe;

    public AvroEntityMapper(KeySchema keySchema, EntitySchema entitySchema, KeySerDe keySerDe, EntitySerDe<E> entitySerDe) {
        this.keySchema = keySchema;
        this.entitySchema = entitySchema;
        this.keySerDe = keySerDe;
        this.entitySerDe = entitySerDe;
    }

    public static class Builder<X extends IndexedRecord> {
        private KeySchema keySchema;
        private EntitySchema entitySchema;
        private KeySerDe keySerDe;
        private EntitySerDe<X> entitySerDe;

        public Builder withKeySchema(KeySchema keySchema) {
            this.keySchema = keySchema;
            return this;
        }

        public Builder withEntitySchema(EntitySchema entitySchema) {
            this.entitySchema = entitySchema;
            return this;
        }

        public Builder withKeySerDe(KeySerDe keySerDe) {
            this.keySerDe = keySerDe;
            return this;
        }

        public Builder withEntitySerDe(EntitySerDe<X> entitySerDe) {
            this.entitySerDe = entitySerDe;
            return this;
        }

        public AvroEntityMapper<X> build() {
            return new AvroEntityMapper<>(keySchema, entitySchema, keySerDe, entitySerDe);
        }
    }


    public AvroEntityComposer<E> getEntityComposer() {
        return entitySerDe.getEntityComposer();
    }

    @Override
    public E mapToEntity(Result result) {
        boolean allNull = true;
        IndexedRecord rowKey = keySerDe.deserialize(result.getRow());
        AvroEntityComposer.Builder<E> builder = getEntityComposer().getBuilder();
        for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
            Object fieldValue;

            if (fieldMapping.mappingType() == MappingType.KEY) {
                fieldValue = rowKey.get(Integer.parseInt(fieldMapping.mappingValue()));
            } else {
                fieldValue = entitySerDe.deserialize(fieldMapping, result);
            }

            if (fieldValue != null) {
                builder.put(fieldMapping.fieldName(), fieldValue);
                // reading a key doesn't count for a row not being null.
                if (fieldMapping.mappingType() != MappingType.KEY) {
                    allNull = false;
                }
            } else if (fieldMapping.defaultValue() != null) {
                builder.put(fieldMapping.fieldName(), fieldMapping.defaultValue());
            }
        }

        /*
         * If all the fields are null, we must assume this is an empty row. There's
         * no way to differentiate between the case where the row exists but this
         * kind of entity wasn't persisted to the row (where the user would expect a
         * return of null), and the case where an entity was put here with all
         * fields set to null.
         *
         * This can also happen if the entity was put with a schema that shares no
         * fields with the current schema, or at the very least, it share no fields
         * that were not null with the current schema.
         *
         * TODO: Think about disallowing puts of all null entity fields.
         */
        return allNull ? null : builder.build();
    }

    @Override
    public Put mapFromEntity(IndexedRecord key, E entity) {
        byte[] keyBytes = keySerDe.serialize(key); // TODO: rethink this
        Put put = new Put(keyBytes);
        for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
            if (fieldMapping.mappingType() == MappingType.KEY) {
                continue;
            }
            Object fieldValue = getEntityComposer().extractField(entity, fieldMapping.fieldName());

            if (fieldValue != null) {
                entitySerDe.serialize(put, fieldMapping, fieldValue);
            }
        }
        return put;
    }

    @Override
    public Set<String> getRequiredColumns() {
        return entitySchema.getRequiredColumns();
    }

    @Override
    public Set<String> getRequiredColumnFamilies() {
        return entitySchema.getRequiredColumnFamilies();
    }

    @Override
    public KeySchema getKeySchema() {
        return keySchema;
    }

    @Override
    public EntitySchema getEntitySchema() {
        return entitySchema;
    }

    @Override
    public KeySerDe getKeySerDe() {
        return keySerDe;
    }

    @Override
    public EntitySerDe<E> getEntitySerDe() {
        return entitySerDe;
    }
}
