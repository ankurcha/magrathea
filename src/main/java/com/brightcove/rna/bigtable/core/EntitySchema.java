package com.brightcove.rna.bigtable.core;

import java.util.*;

/**
 * An EntitySchema is the parsed schema that contains the properties of an HBase
 * Common entity schema.
 */
public class EntitySchema {

    private final Collection<String> tables;
    private final Map<String, FieldMapping> fieldMappings = new HashMap<>();
    private final String rawSchema;
    private final String name;

    /**
     * Constructs the EntitySchema
     *
     * @param tables        The tables this EntitySchema can be persisted to
     * @param name          The name of the entity schema
     * @param rawSchema     The raw schema type that underlies the EntitySchema implementation
     * @param fieldMappings The list of FieldMappings that specify how each field maps to an
     *                      HBase row
     */
    public EntitySchema(Collection<String> tables, String name, String rawSchema, Collection<FieldMapping> fieldMappings) {
        this.tables = tables;
        this.name = name;
        this.rawSchema = rawSchema;
        for (FieldMapping fieldMapping : fieldMappings) {
            this.fieldMappings.put(fieldMapping.fieldName(), fieldMapping);
        }
    }

    /**
     * Get the tables this EntitySchema can be persisted to.
     *
     * @return The list of tables.
     */
    public Collection<String> getTables() {
        return tables;
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
        Set<String> set = new HashSet<String>();
        Set<String> columnSet = getRequiredColumns();
        for (String column : columnSet) {
            set.add(column.split(":")[0]);
        }
        return set;
    }

    /**
     * Method meant to determine if two EntitySchemas are compatible with each
     * other for schema migration purposes. Classes that inherit EntitySchema
     * should override this implementation, since this implemetnation isn't able
     * to make that determination.
     * <p>
     * TODO: Figure out a base set of properties that all entity schema
     * implementations should share in their implementation of determining
     * compatibility and execute that here.
     *
     * @param entitySchema The other EntitySchema to determine compatible with
     * @return
     */
    public boolean compatible(EntitySchema entitySchema) {
        // throw an exception if anyone calls this directly, as this should be
        // overridden in derived classes.
        throw new UnsupportedOperationException(
            "EntityScheam class can't determine if two entity schemas are compatible.");
    }
}
