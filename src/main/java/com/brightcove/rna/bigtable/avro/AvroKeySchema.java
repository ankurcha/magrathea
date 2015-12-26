package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.KeySchema;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

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
        List<Field> fieldsPartOfKey = new ArrayList<>();
        Map<String, Field> fileToFieldInstance = schema.getFields().stream()
                                                                   .collect(toMap(Field::name, Function.identity()));
        // sort and then create a schema for the row key
        keyFieldMappings.stream()
            .sorted((o1, o2) -> {
                int i1 = Integer.parseInt(o1.mappingValue());
                int i2 = Integer.parseInt(o2.mappingValue());
                return Integer.compare(i1, i2);
            })
            .map(fieldMapping -> fileToFieldInstance.get(fieldMapping.fieldName()))
            .forEach(field -> fieldsPartOfKey.add(AvroUtils.cloneField(field)));

        this.schema = Schema.createRecord(fieldsPartOfKey);
    }

    public AvroKeySchema(Schema schema, String rawSchema) {
        super(rawSchema);
        this.schema = schema;
    }
}
