package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.EntitySchema;
import com.brightcove.rna.bigtable.core.FieldMapping;
import org.apache.avro.Schema;

import java.util.Collection;

/**
 * An EntitySchema implementation powered by Avro.
 */
public class AvroEntitySchema extends EntitySchema {

    private final Schema schema;

    /**
     * Constructor for the AvroEntitySchema.
     *
     * @param schema        The Avro Schema that underlies this EntitySchema implementation
     * @param rawSchema     The Avro Schema as a string that underlies the EntitySchema implementation
     * @param fieldMappings The list of FieldMappings that specify how each field maps to an HBase row
     */
    public AvroEntitySchema(Schema schema, String rawSchema, Collection<FieldMapping> fieldMappings) {
        super(schema.getName(), rawSchema, fieldMappings);
        this.schema = schema;
    }

    /**
     * Get the Avro Schema that underlies this EntitySchema implementation.
     *
     * @return The Avro Schema
     */
    public Schema getAvroSchema() {
        return schema;
    }

}
