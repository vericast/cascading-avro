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

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Adapts sink fields to match wrapped scheme.
 */
public class RenamerScheme extends Scheme {
	private final Scheme scheme;
	private final Fields toSinkFields;

	public RenamerScheme(Scheme scheme, Fields from, Fields to) {
		super(scheme.getSourceFields(), from);
		this.scheme = scheme;
		this.toSinkFields = to;
		if (scheme.getSinkFields().select(to).size() != to.size()) {
			throw new IllegalArgumentException("Can't use " + to + " with "
					+ scheme.getSinkFields());
		}
	}

	@Override
	public void sourceInit(Tap tap, JobConf conf) throws IOException {
		scheme.sourceInit(tap, conf);
	}

	@Override
	public void sinkInit(Tap tap, JobConf conf) throws IOException {
		scheme.sinkInit(tap, conf);
	}

	@Override
	public Tuple source(Object key, Object value) {
		return scheme.source(key, value);
	}

	@Override
	public void sink(TupleEntry tupleEntry, OutputCollector collector)
			throws IOException {
		final Tuple tuple = tupleEntry.selectTuple(this.getSinkFields());
		scheme.sink(new TupleEntry(toSinkFields, tuple), collector);
	}
}
