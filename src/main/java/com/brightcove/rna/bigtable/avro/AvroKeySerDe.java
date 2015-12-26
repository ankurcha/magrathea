package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.KeySerDe;
import com.brightcove.rna.bigtable.avro.io.MemcmpDecoder;
import com.brightcove.rna.bigtable.avro.io.MemcmpEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.UNION;

public class AvroKeySerDe implements KeySerDe {
    private final Schema schema;
    private final Schema[] partialSchemas;

    public AvroKeySerDe(Schema schema) {
        this.schema = schema;
        int fieldSize = schema.getFields().size();
        this.partialSchemas = new Schema[fieldSize];
        for (int i = 0; i < fieldSize; i++) {
            if (i == (fieldSize - 1)) {
                break;
            }
            List<Schema.Field> partialFieldList = schema.getFields().subList(0, i + 1).stream()
                                  .map(AvroUtils::cloneField)
                                  .collect(Collectors.toList());
            this.partialSchemas[i] = Schema.createRecord(partialFieldList);
        }
    }


    @Override
    public byte[] serialize(IndexedRecord key) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new MemcmpEncoder(outputStream);
        Schema schemaToUse;
        int keySchemaLength = key.getSchema().getFields().size();
        if (keySchemaLength == schema.getFields().size()) {
            schemaToUse = schema;
        } else {
            schemaToUse = partialSchemas[keySchemaLength - 1];
        }
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schemaToUse);
        GenericRecord record = new GenericData.Record(schemaToUse);
        for (int i = 0; i < keySchemaLength; i++) {
            Object keyPart = key.get(i);
            if (keyPart == null) {
                // keyPart is null, let's make sure we check that the key can support a
                // null value so we can throw a friendly exception if it can't.
                Schema fieldSchema = schemaToUse.getFields().get(i).schema();
                Schema.Type type = fieldSchema.getType();
                if (type != NULL && type != UNION) {
                    throw new IllegalStateException("Null key field only supported in null type or union type that has a null type.");
                } else if (type == UNION) {
                    boolean foundNullInUnion = false;
                    for (Schema unionSchema : fieldSchema.getTypes()) {
                        if (unionSchema.getType() == NULL) {
                            foundNullInUnion = true;
                        }
                    }
                    if (!foundNullInUnion) {
                        throw new IllegalStateException("Null key field only supported in union type that has a null type.");
                    }
                }
            }
            record.put(i, keyPart);
        }
        AvroUtils.writeAvroEntity(record, encoder, datumWriter);
        return outputStream.toByteArray();
    }

    @Override
    public IndexedRecord deserialize(byte[] keyBytes) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(keyBytes);
        Decoder decoder = new MemcmpDecoder(inputStream);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        return AvroUtils.readAvroEntity(decoder, datumReader);
    }
}
