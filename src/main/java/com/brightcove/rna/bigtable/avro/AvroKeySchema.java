package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.KeySchema;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.ArrayList;
import java.util.List;

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
        for (Field field : schema.getFields()) {
            for (FieldMapping fieldMapping : keyFieldMappings) {
                if (field.name().equals(fieldMapping.fieldName())) {
                    fieldsPartOfKey.add(AvroUtils.cloneField(field));
                }
            }
        }
        this.schema = Schema.createRecord(fieldsPartOfKey);
    }

    public AvroKeySchema(Schema schema, String rawSchema) {
        super(rawSchema);
        this.schema = schema;
    }
}
