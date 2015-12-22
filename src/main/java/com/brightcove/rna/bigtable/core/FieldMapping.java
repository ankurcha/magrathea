package com.brightcove.rna.bigtable.core;

import org.immutables.value.Value;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A field mapping represents a type that specifies how a schema field maps to a column in HBase.
 */
@Value.Immutable
public abstract class FieldMapping {

    public abstract String      fieldName();
    public abstract MappingType mappingType();
    public abstract String      mappingValue();
    public abstract Object      defaultValue();

    @Value.Derived
    public byte[] family() { return getFamilyFromMappingValue(mappingValue()); }

    @Value.Derived
    public byte[] qualifier() { return getQualifierFromMappingValue(mappingValue()); }

    private byte[] getFamilyFromMappingValue(String mappingValue) {
        if (mappingType() == MappingType.KEY) {
            return null;
        }
        String[] familyQualifier = mappingValue.split(":", 2);
        return familyQualifier[0].getBytes(UTF_8);
    }

    private byte[] getQualifierFromMappingValue(String mappingValue) {
        if (mappingType() == MappingType.KEY) {
            return null;
        }
        String[] familyQualifier = mappingValue.split(":", 2);
        return familyQualifier.length == 1 ? new byte[0] : familyQualifier[1].getBytes(UTF_8);
    }

}
