/*
* Copyright (c) 2012 MaxPoint Interactive, Inc. All Rights Reserved.
*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.maxpoint.cascading.avro;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import org.apache.avro.Schema;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * Class AvroAdapterBase
 */
public abstract class AvroSchemeBase extends Scheme {
    /**
     * Extracts serialization info from Avro schema
     */
    protected LinkedHashMap<String, FieldType> parseSchema(Schema avroSchema, Set<Schema.Type> allowedTypes) {
        if(avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Base schema must be of type RECORD, found " + avroSchema.getType());
        }

        final LinkedHashMap<String, FieldType> fields = new LinkedHashMap<String, FieldType>();
        final List<Schema.Field> schemaFields = avroSchema.getFields();
        for(Schema.Field field : schemaFields) {
            final Schema.Type type = field.schema().getType();
            if(!allowedTypes.contains(type)) {
                throw new IllegalArgumentException("Don't know how to handle schema with " + field.name() + " of type " + type);
            }
            fields.put(field.name(), typeInfo(field));
        }
        return fields;
    }

    private FieldType typeInfo(Schema.Field field) {
        final Schema schema = field.schema();
        final Schema.Type type = schema.getType();
        // special case [type, null] unions
        if(type == Schema.Type.UNION) {
            return new FieldType(resolveUnion(schema), true, field.pos(), schema);
        } else {
            return new FieldType(type, type == Schema.Type.NULL, field.pos(), schema);
        }
    }

    protected Schema.Type resolveUnion(Schema schema) {
        final List<Schema> components = schema.getTypes();
        if(components.size() == 2) {
            final Schema s0 = components.get(0), s1 = components.get(1);
            if(s0.getType() == Schema.Type.NULL) {
                return s1.getType();
            } else if(s1.getType() == Schema.Type.NULL) {
                return s0.getType();
            }
        }
        throw new IllegalArgumentException("Can't parse " + schema);
    }
    
    protected Class<?>[] inferClasses(Collection<FieldType> types) {
        Class<?>[] result = new Class<?>[types.size()];
        int ix = 0;
        for(FieldType typeInfo : types) {
            result[ix++] = inferClass(typeInfo);
        }
        return result;
    }

    protected Class<?> inferClass(FieldType typeInfo) {
        switch(typeInfo.type) {
            case BOOLEAN:
                return Boolean.class;
            case BYTES:
                return BytesWritable.class;
            case DOUBLE:
                return Double.class;
            case FIXED:
                return BytesWritable.class;
            case FLOAT:
                return Float.class;
            case INT:
                return Integer.class;
            case LONG:
                return Long.class;
            case NULL:
                return Object.class;
            case STRING:
                return String.class;
        }
        throw new IllegalArgumentException("Can't resolve " + typeInfo.type + " to java class");
    }

    protected Fields fields(LinkedHashMap<String, FieldType> schemaFields) {
        final Set<String> names = schemaFields.keySet();
        return new Fields(names.toArray(new String[names.size()]));
    }
    
    static Schema readSchema(ObjectInputStream in) throws IOException {
//        final Schema.Parser parser = new Schema.Parser();
        return Schema.parse(in.readUTF());
    }

    final class FieldType implements Serializable {
        public boolean isNullable;
        public Schema.Type type;
        public Schema schema;
        public int pos;

        private FieldType(Schema.Type type, boolean nullable, int pos, Schema schema) {
            this.type = type;
            isNullable = nullable;
            this.pos = pos;
            this.schema = schema;
        }

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeBoolean(isNullable);
            out.writeObject(type);
            out.writeInt(pos);
            out.writeUTF(schema.toString());
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            isNullable = in.readBoolean();
            type = (Schema.Type)in.readObject();
            pos = in.readInt();
            schema = readSchema(in);
        }
    }
}
