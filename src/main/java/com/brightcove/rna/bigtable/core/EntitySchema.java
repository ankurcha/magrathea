package com.brightcove.rna.bigtable.core;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.brightcove.rna.bigtable.core.MappingType.*;

/**
 * An EntitySchema is the parsed schema that contains the properties of an HBase
 * Common entity schema.
 */
public class EntitySchema {

    private final Map<String, FieldMapping> fieldMappings;
    private final String rawSchema;
    private final String name;

    /**
     * Constructs the EntitySchema
     *
     * @param name          The name of the entity schema
     * @param rawSchema     The raw schema type that underlies the EntitySchema implementation
     * @param fieldMappings The list of FieldMappings that specify how each field maps to an HBase row
     */
    public EntitySchema(String name, String rawSchema, Collection<FieldMapping> fieldMappings) {
        this.name = name;
        this.rawSchema = rawSchema;
        this.fieldMappings = fieldMappings.stream().collect(Collectors.toMap(FieldMapping::fieldName, Function.identity()));
    }

    /**
     * Get the name of this EntitySchema
     *
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the FieldMapping for the specified fieldName. Returns null if one
     * doesn't exist.
     *
     * @param fieldName The field name to get the FieldMapping for
     * @return The FieldMapping, or null if one doesn't exist fo rthe fieldName.
     */
    public FieldMapping getFieldMapping(String fieldName) {
        return fieldMappings.get(fieldName);
    }

    /**
     * Get the FieldMappings for this schema.
     *
     * @return The collection of FieldMappings
     */
    public Collection<FieldMapping> getFieldMappings() {
        return fieldMappings.values();
    }

    /**
     * Get the raw schema that was parsed to create this schema.
     *
     * @return The raw scheam.
     */
    public String getRawSchema() {
        return rawSchema;
    }

    /**
     * Get the HBase columns required by this schema.
     *
     * @return The set of columns
     */
    public Set<String> getRequiredColumns() {
        // convert to a set based on mapping type ( 'key' type is null )
        return fieldMappings.values().stream()
            .map(fm -> {
                if(fm.mappingType() == COLUMN || fm.mappingType() == COUNTER) {
                    return fm.mappingValue();
                } else if(fm.mappingType() == KEY_AS_COLUMN) {
                    return fm.mappingValue().split(":", 1)[0] + ":";
                }
                return null;
            })
            .filter(val -> val!=null) // remove all nulls
            .collect(Collectors.toSet()); // return as a set
    }

    /**
     * Get the HBase column families required by this schema.
     *
     * @return The set of column families.
     */
    public Set<String> getRequiredColumnFamilies() {
        return getRequiredColumns().stream()
                .map(column -> column.split(":")[0])
                .collect(Collectors.toSet());
    }

}
