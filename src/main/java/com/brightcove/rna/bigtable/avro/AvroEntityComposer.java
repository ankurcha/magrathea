package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.DatasetException;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.MappingType;
import com.brightcove.rna.bigtable.core.SchemaValidationException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An EntityComposer implementation for Avro records.
 *
 * @param <E> The type of the entity
 */
public class AvroEntityComposer<E extends IndexedRecord> {

    private final AvroEntitySchema avroSchema;
    private final boolean specific;
    private final AvroRecordBuilderFactory<E> recordBuilderFactory;

    /**
     * A mapping of entity field names to AvroRecordBuilderFactories for any
     * keyAsColumn mapped fields that are Avro record types. These are needed to
     * get builders that can construct the keyAsColumn field values from their
     * parts.
     */
    private final Map<String, AvroRecordBuilderFactory<E>> kacRecordBuilderFactories;

    /**
     * The number of key parts in the entity schema.
     */
    private final int keyPartCount;

    /**
     * AvroEntityComposer constructor.
     *
     * @param avroEntitySchema The schema for the Avro entities this composer composes.
     * @param specific         True if this composer composes Specific records. Otherwise, it composes Generic records.
     */
    public AvroEntityComposer(AvroEntitySchema avroEntitySchema, boolean specific) {
        this.avroSchema = avroEntitySchema;
        this.specific = specific;
        this.recordBuilderFactory = buildAvroRecordBuilderFactory(avroEntitySchema.getAvroSchema());
        this.kacRecordBuilderFactories = new HashMap<>();
        int keyPartCount = 0;
        for (FieldMapping fieldMapping : avroEntitySchema.getFieldMappings()) {
            if (fieldMapping.mappingType() == MappingType.KEY) {
                keyPartCount++;
            }
        }
        this.keyPartCount = keyPartCount;
        initRecordBuilderFactories();
    }

    /**
     * An interface for entity builders.
     *
     * @param <E> The type of the entity this builder builds.
     */
    public interface Builder<E> {

        /**
         * Put a field value into the entity.
         *
         * @param fieldName The name of the field
         * @param value     The value of the field
         * @return A reference to the Builder, so puts can be chained.
         */
        Builder<E> put(String fieldName, Object value);

        /**
         * Builds the entity, and returns it.
         *
         * @return The built entity
         */
        E build();
    }

    public Builder<E> getBuilder() {
        return new Builder<E>() {
            private final AvroRecordBuilder<E> recordBuilder = recordBuilderFactory.getBuilder();

            @Override
            public Builder<E> put(
                String fieldName, Object value) {
                recordBuilder.put(fieldName, value);
                return this;
            }

            @Override
            public E build() {
                return recordBuilder.build();
            }
        };
    }

    public Object extractField(E entity, String fieldName) {
        Schema schema = avroSchema.getAvroSchema();
        Field field = schema.getField(fieldName);
        checkNotNull(field, "No field named %s in schema %s", fieldName, schema);
        Object fieldValue = entity.get(field.pos());
        if (fieldValue == null) {
            // if the field value is null, and the field is a primitive type,
            // we should make the field represent java's default type. This
            // can happen when using GenericRecord. SpecificRecord has it's
            // fields represented by members of a class, so a SpecificRecord's
            // primitive fields will never be null. We are doing this so
            // GenericRecord acts like SpecificRecord in this case.
            fieldValue = getDefaultPrimitive(field);
        }
        return fieldValue;
    }

    @SuppressWarnings("unchecked")
    public Map<CharSequence, Object> extractKeyAsColumnValues(String fieldName, Object fieldValue) {
        Schema schema = avroSchema.getAvroSchema();
        Field field = schema.getField(fieldName);
        checkNotNull(field, "No field named %s in schema %s", fieldName, schema);
        if (field.schema().getType() == Schema.Type.MAP) {
            return new HashMap<>((Map<CharSequence, Object>) fieldValue);
        } else if (field.schema().getType() == Schema.Type.RECORD) {
            Map<CharSequence, Object> keyAsColumnValues = new HashMap<>();
            IndexedRecord avroRecord = (IndexedRecord) fieldValue;
            for (Field avroRecordField : avroRecord.getSchema().getFields()) {
                keyAsColumnValues.put(avroRecordField.name(), avroRecord.get(avroRecordField.pos()));
            }
            return keyAsColumnValues;
        } else {
            throw new SchemaValidationException(
                String.format("Only MAP or RECORD type valid for keyAsColumn fields. Found %s", field.schema().getType()));
        }
    }

    public Object buildKeyAsColumnField(String fieldName, Map<CharSequence, Object> keyAsColumnValues) {
        Schema schema = avroSchema.getAvroSchema();
        Field field = schema.getField(fieldName);
        checkNotNull(field, "No field named %s in schema %s", fieldName, schema);

        Schema.Type fieldType = field.schema().getType();
        if (fieldType == Schema.Type.MAP) {
            Map<CharSequence, Object> retMap = new HashMap<>();
            for (Entry<CharSequence, Object> entry : keyAsColumnValues.entrySet()) {
                retMap.put(entry.getKey(), entry.getValue());
            }
            return retMap;
        } else if (fieldType == Schema.Type.RECORD) {
            AvroRecordBuilder<E> builder = kacRecordBuilderFactories.get(fieldName).getBuilder();
            for (Entry<CharSequence, Object> keyAsColumnEntry : keyAsColumnValues.entrySet()) {
                builder.put(keyAsColumnEntry.getKey().toString(), keyAsColumnEntry.getValue());
            }
            return builder.build();
        } else {
            throw new SchemaValidationException(
                String.format("Only MAP or RECORD type valid for keyAsColumn fields. Found %s", fieldType));
        }
    }

    /**
     * Initialize the AvroRecordBuilderFactories for all keyAsColumn mapped fields
     * that are record types. We need to be able to get record builders for these
     * since the records are broken across many columns, and need to be
     * constructed by the composer.
     */
    private void initRecordBuilderFactories() {
        avroSchema.getFieldMappings()
            .stream()
            .filter(fieldMapping -> fieldMapping.mappingType() == MappingType.KEY_AS_COLUMN)
            .forEach(fieldMapping -> {
                String fieldName = fieldMapping.fieldName();
                Schema fieldSchema = avroSchema.getAvroSchema().getField(fieldName).schema();
                Schema.Type fieldSchemaType = fieldSchema.getType();
                if (fieldSchemaType == Schema.Type.RECORD) {
                    AvroRecordBuilderFactory<E> factory = buildAvroRecordBuilderFactory(fieldSchema);
                    kacRecordBuilderFactories.put(fieldName, factory);
                }
            });
    }

    /**
     * Build the appropriate AvroRecordBuilderFactory for this instance. Avro has
     * many different record types, of which we support two: Specific and Generic.
     *
     * @param schema The Avro schema needed to construct the AvroRecordBuilderFactory.
     * @return The constructed AvroRecordBuilderFactory.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private AvroRecordBuilderFactory<E> buildAvroRecordBuilderFactory(Schema schema) {
        if (!specific) {
            return (AvroRecordBuilderFactory<E>) new GenericAvroRecordBuilderFactory(schema);
        }
        Class<E> specificClass;
        String className = schema.getFullName();
        try {
            specificClass = (Class<E>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new DatasetException(String.format("Could not get Class instance for %s", className));
        }
        return new SpecificAvroRecordBuilderFactory(specificClass);
    }

    /**
     * Get's the default value for the primitive types. This matches the default
     * Java would assign to the following primitive types:
     * <p>
     * int, long, boolean, float, and double.
     * <p>
     * If field is any other type, this method will return null.
     *
     * @param field The Schema field
     * @return The default value for the schema field's type, or null if the type of field is not a primitive type.
     */
    private Object getDefaultPrimitive(Schema.Field field) {
        switch (field.schema().getType()) {
            case INT:
                return 0;
            case LONG:
                return 0L;
            case BOOLEAN:
                return false;
            case FLOAT:
                return 0.0F;
            case DOUBLE:
                return 0.0D;
            default:
                return null; // not a primitive type, so return null
        }
    }

    public List<Object> getPartitionKeyParts(E entity) {
        Object[] parts = new Object[keyPartCount];
        avroSchema.getFieldMappings()
            .stream()
            .filter(fieldMapping -> fieldMapping.mappingType() == MappingType.KEY)
            .forEach(fieldMapping -> {
                int pos = avroSchema.getAvroSchema().getField(fieldMapping.fieldName()).pos();
                parts[Integer.parseInt(fieldMapping.mappingValue())] = entity.get(pos);
            });
        return Arrays.asList(parts);
    }
}
