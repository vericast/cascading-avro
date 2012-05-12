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

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Cascading scheme for reading data serialized using Avro. This scheme sources and sinks tuples with fields named
 * and ordered the same was as the Avro schema used in the constructor.
 * <p>
 * The following Avro types are supported:
 * <ul>
 *     <li>boolean</li>
 *     <li>bytes (as BytesWritable)</li>
 *     <li>double</li>
 *     <li>fixed (as BytesWritable)</li>
 *     <li>float</li>
 *     <li>int</li>
 *     <li>long</li>
 *     <li>null</li>
 *     <li>string</li>
 *     <li>array</li>
 *     <li>map</li>
 *     <li>union of [type, null], treated as nullable value of the type</li>
 * </ul>
 */



@SuppressWarnings("deprecation")
public class AvroScheme extends AvroSchemeBase {
    public static final EnumSet<Schema.Type> ALLOWED_TYPES = EnumSet.of(Schema.Type.BOOLEAN, Schema.Type.BYTES,
            Schema.Type.DOUBLE, Schema.Type.FIXED, Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.LONG,
            Schema.Type.NULL, Schema.Type.STRING, Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP);

    private Schema dataSchema;
    private FieldType[] fieldTypes;
    private transient IndexedRecord cached;
    private Object[] buffer;
    
    public AvroScheme(Schema dataSchema) {
        this.dataSchema = dataSchema;
        final LinkedHashMap<String, FieldType> schemaFields = parseSchema(dataSchema, ALLOWED_TYPES);

        final Fields fields = fields(schemaFields);
        setSinkFields(fields);
        setSourceFields(fields);

        final Collection<FieldType> types = schemaFields.values();
        fieldTypes = types.toArray(new FieldType[types.size()]); 
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        conf.set(AvroJob.INPUT_SCHEMA, dataSchema.toString());
        conf.setInputFormat(AvroInputFormat.class);
        addAvroSerialization(conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple source(Object key, Object value) {
        final AvroWrapper<IndexedRecord> wrapper = (AvroWrapper<IndexedRecord>) key;
        final IndexedRecord record = wrapper.datum();

        final Tuple result = Tuple.size(getSourceFields().size());
        for(int i = 0; i < fieldTypes.length; i++) {
            final Object val = fromAvro(fieldTypes[i], record.get(fieldTypes[i].pos));
            result.set(i, val);
        }
        return result;
    }

    private Object fromAvro(FieldType typeInfo, Object val) {
        if(val == null) {
            return null;
        }
        switch(typeInfo.type) {
            case STRING:
                return val.toString();
            case FIXED:
                return new BytesWritable(((GenericFixed)val).bytes());
            case BYTES:
                return bytesWritable((ByteBuffer)val);
        }
        return val;
    }

    private BytesWritable bytesWritable(ByteBuffer val) {
        final byte[] data = new byte[val.remaining()];
        val.get(data);
        return new BytesWritable(data);
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.set(AvroJob.OUTPUT_SCHEMA, dataSchema.toString());
        conf.setOutputFormat(AvroOutputFormat.class);
        conf.setOutputKeyClass(AvroWrapper.class);

        // set compression
        AvroOutputFormat.setDeflateLevel(conf, 6);
        AvroJob.setOutputCodec(conf, DataFileConstants.DEFLATE_CODEC);
        AvroOutputFormat.setSyncInterval(conf, 1048576);
    }

    private void addAvroSerialization(JobConf conf) {
        // add AvroSerialization to io.serializations
        final Collection<String> serializations = conf.getStringCollection("io.serializations");
        if (!serializations.contains(AvroSerialization.class.getName())) {
            serializations.add(AvroSerialization.class.getName());
            conf.setStrings("io.serializations", serializations.toArray(new String[serializations.size()]));
        }
        
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector output) throws IOException {
        cached = (IndexedRecord)SpecificData.get().newRecord(cached, dataSchema);

        final Fields sinkFields = getSinkFields();
        for(int i = 0; i < fieldTypes.length; i++) {
            final Comparable field = sinkFields.get(i);
            final Object val = tupleEntry.getObject(field);
            cached.put(fieldTypes[i].pos, toAvro(field, fieldTypes[i], val));
        }
        output.collect(new AvroWrapper<IndexedRecord>(cached), NullWritable.get());
    }
    
    private Object toAvro(Comparable field, FieldType typeInfo, Object val) throws IOException {
        if(val == null) {
            if(typeInfo.isNullable) {
                return null;
            } else {
                throw new NullPointerException("Field " + field + " is not nullable");
            }
        }
        
        switch(typeInfo.type) {
            case STRING:
                return val.toString();
            case FIXED:
                return SpecificData.get().createFixed(null, ((BytesWritable)val).getBytes(), typeInfo.schema);
            case BYTES:
                return ByteBuffer.wrap(((BytesWritable)val).getBytes());
            case LONG:
                return ((Number)val).longValue();
            case INT:
                return ((Number)val).intValue();
            case DOUBLE:
                return ((Number)val).doubleValue();
            case FLOAT:
                return ((Number)val).floatValue();

        }
        return val;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(this.fieldTypes);
        out.writeUTF(this.dataSchema.toString());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.fieldTypes = (FieldType[])in.readObject();
        this.dataSchema = readSchema(in);
    }

}
