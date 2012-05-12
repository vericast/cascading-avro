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
import cascading.tap.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.avro.Schema;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class TextSchemeTest
 */
public class TextSchemeTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testRoundTrip() throws Exception {
//        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = Schema.parse(getClass().getResourceAsStream("test2.avsc"));
        final TextScheme scheme = new TextScheme(schema);

        final Lfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
        final TupleEntryCollector collector = lfs.openForWrite(new JobConf());
        final Fields fields = new Fields("aBoolean", "anInt", "aLong", "aDouble", "aFloat", "aNull", "aString");
        write(scheme, collector, new TupleEntry(fields, new Tuple(false, 1, 2L, 3.0, 4.0F, null, "test-string")));
        write(scheme, collector, new TupleEntry(fields, new Tuple(false, 1, 2L, 3.0, 4.0F, null, null)));
        collector.close();

        final TupleEntryIterator iterator = lfs.openForRead(new JobConf());
        assertTrue(iterator.hasNext());
        final TupleEntry readEntry1 = iterator.next();

        assertEquals(false, readEntry1.getBoolean("aBoolean"));
        assertEquals(1, readEntry1.getInteger("anInt"));
        assertEquals(2L, readEntry1.getLong("aLong"));
        assertEquals(3.0, readEntry1.getDouble("aDouble"), 0.01);
        assertEquals(4.0F, readEntry1.getFloat("aFloat"), 0.01);
        assertEquals("test-string", readEntry1.get("aString"));

        assertTrue(iterator.hasNext());
        final TupleEntry readEntry2 = iterator.next();

        assertNull(readEntry2.get("aString"));
    }

    private void write(Scheme scheme, TupleEntryCollector collector, TupleEntry te) {
        collector.add(te.selectTuple(scheme.getSinkFields()));
    }

}
