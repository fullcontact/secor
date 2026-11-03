/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts records of any schema version to a file's schema using Avro schema resolution
 * rules: fields missing from the record take their defaults, and fields unknown to the file
 * schema are dropped. Not thread-safe; use one instance per file writer.
 */
public class AvroRecordResolver {

    private final Schema fileSchema;
    private final Map<Schema, SpecificDatumReader<GenericRecord>> resolvingReaders = new HashMap<>();

    public AvroRecordResolver(Schema fileSchema) {
        this.fileSchema = fileSchema;
    }

    public Schema getFileSchema() {
        return fileSchema;
    }

    /**
     * Returns the record as an instance of the file schema, or {@code null} for a {@code null} record.
     *
     * @throws IOException if the record's schema cannot be resolved to the file schema, such as
     *                     when the file schema has a field without a default that the record lacks
     */
    public GenericRecord resolve(GenericRecord record) throws IOException {
        if (record == null || record.getSchema().equals(fileSchema)) {
            return record;
        }
        // Avro only resolves schemas while decoding, so the record goes through a binary round trip.
        Schema recordSchema = record.getSchema();
        byte[] payload = AvroSerializer.serialize(new SpecificDatumWriter<>(recordSchema), record);
        SpecificDatumReader<GenericRecord> reader = resolvingReaders.computeIfAbsent(recordSchema,
                writerSchema -> new SpecificDatumReader<>(writerSchema, fileSchema));
        return reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
    }
}
