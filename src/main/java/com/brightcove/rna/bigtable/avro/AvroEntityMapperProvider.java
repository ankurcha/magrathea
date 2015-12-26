package com.brightcove.rna.bigtable.avro;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AvroEntityMapperProvider {
    private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

    public static <E extends SpecificRecord> AvroEntityMapper<E> forClass(Class<E> clazz) {
        Schema schema = resolveSchemaFromClass(clazz);
        Preconditions.checkNotNull(schema, "Unable to resolve avro schmea for class %s", clazz.toString());

        return forSchemaAndClass(schema.toString(), clazz);
    }

    public static <E extends SpecificRecord> AvroEntityMapper<E> forSchemaAndClass(String schemaStr, Class<E> clazz) {
        // get entity schema
        AvroEntitySchema readerSchema = parser.parseEntitySchema(schemaStr);
        readerSchema = AvroUtils.mergeSpecificStringTypes(clazz, readerSchema);
        AvroEntityComposer<E> entityComposer = new AvroEntityComposer<>(readerSchema, true);
        AvroEntitySerDe<E> entitySerDe = new AvroEntitySerDe<>(entityComposer, readerSchema, true);

        // get key serde
        AvroKeySchema keySchema = parser.parseKeySchema(schemaStr);
        keySchema = AvroUtils.mergeSpecificStringTypes(clazz, keySchema);
        AvroKeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema());

        return new AvroEntityMapper<>(keySchema, readerSchema, keySerDe, entitySerDe);
    }

    static <E> Schema resolveSchemaFromClass(Class<E> type) {
        Schema readerSchema = null;
        GenericData dataModel = getDataModelForType(type);

        if (dataModel instanceof SpecificData) {
            readerSchema = ((SpecificData) dataModel).getSchema(type);
        }

        return readerSchema;
    }


    static <E> GenericData getDataModelForType(Class<E> type) {
        // Need to check if SpecificRecord first because specific records also
        // implement GenericRecord
        if (SpecificRecord.class.isAssignableFrom(type)) {
            return new SpecificData(type.getClassLoader());
        } else if (IndexedRecord.class.isAssignableFrom(type)) {
            return GenericData.get();
        } else {
            return AllowNulls.get();
        }
    }

    static class AllowNulls extends ReflectData {
        private static final AllowNulls INSTANCE = new AllowNulls();

        /**
         * Return the singleton instance.
         */
        public static AllowNulls get() {
            return INSTANCE;
        }

        @Override
        protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
            Schema schema = super.createFieldSchema(field, names);
            if (field.getType().isPrimitive()) {
                return schema;
            }
            return makeNullableSchema(schema);
        }

        /**
         * Create and return a union of the null schema and the provided schema.
         */
        public static Schema makeNullableSchema(Schema schema) {
            if (schema.getType() == Schema.Type.UNION) {
                // check to see if the union already contains NULL
                for (Schema subType : schema.getTypes()) {
                    if (subType.getType() == Schema.Type.NULL) {
                        return schema;
                    }
                }
                // add null as the first type in a new union
                List<Schema> withNull = new ArrayList<Schema>();
                withNull.add(Schema.create(Schema.Type.NULL));
                withNull.addAll(schema.getTypes());
                return Schema.createUnion(withNull);
            } else {
                // create a union with null
                return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
                    schema));
            }
        }
    }
}

