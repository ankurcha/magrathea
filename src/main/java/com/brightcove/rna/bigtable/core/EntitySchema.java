package com.brightcove.rna.bigtable.core;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An EntitySchema is the parsed schema that contains the properties of an HBase
 * Common entity schema.
 */
public class EntitySchema {

    private final Map<String, FieldMapping> fieldMappings = new HashMap<>();
    private final String rawSchema;
    private final String name;

    /**
     * Constructs the EntitySchema
     *
     * @param name          The name of the entity schema
     * @param rawSchema     The raw schema type that underlies the EntitySchema implementation
     * @param fieldMappings The list of FieldMappings that specify how each field maps to an
     *                      HBase row
     */
    public EntitySchema(String name, String rawSchema, Collection<FieldMapping> fieldMappings) {
        this.name = name;
        this.rawSchema = rawSchema;
        for (FieldMapping fieldMapping : fieldMappings) {
            this.fieldMappings.put(fieldMapping.fieldName(), fieldMapping);
        }
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
        Set<String> set = new HashSet<>();
        for (FieldMapping fieldMapping : fieldMappings.values()) {
            switch (fieldMapping.mappingType()) {
                case KEY:
                    break;
                case COLUMN:
                case COUNTER:
                    set.add(fieldMapping.mappingValue());
                    break;
                case KEY_AS_COLUMN:
                    String family = fieldMapping.mappingValue().split(":", 1)[0];
                    family = family + ":";
                    set.add(family);
                    break;
            }
        }
        return set;
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
