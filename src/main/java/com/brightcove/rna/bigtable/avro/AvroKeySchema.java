package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.KeySchema;
import com.google.common.base.Objects;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A KeySchema implementation powered by Avro.
 */
public class AvroKeySchema extends KeySchema {

    private final Schema schema;

    public Schema getAvroSchema() {
        return schema;
    }

    /**
     * Constructor for the AvroKeySchema.
     *
     * @param schema    The Avro Schema that underlies this KeySchema implementation
     * @param rawSchema The Avro Schema as a string that underlies the KeySchema implementation
     */
    public AvroKeySchema(Schema schema, String rawSchema, List<FieldMapping> keyFieldMappings) {
        super(rawSchema);
        Map<String, FieldMapping> fieldNameToFieldMapping = keyFieldMappings.stream()
            .collect(Collectors.toMap(FieldMapping::fieldName, Function.identity()));
        List<Field> fieldsPartOfKey = schema.getFields().stream()
            .filter(field -> fieldNameToFieldMapping.containsKey(field.name()))
            .collect(Collectors.toList());
        this.schema = Schema.createRecord(fieldsPartOfKey);
    }

    public AvroKeySchema(Schema schema, String rawSchema) {
        super(rawSchema);
        this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvroKeySchema that = (AvroKeySchema) o;
        return Objects.equal(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema);
    }
}
