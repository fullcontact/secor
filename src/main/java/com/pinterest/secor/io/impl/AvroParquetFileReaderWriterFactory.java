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
package com.pinterest.secor.io.impl;

import com.pinterest.secor.common.AvroSchemaRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.AvroSerializer;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroSchemaRegistryFactory;
import com.pinterest.secor.util.ParquetUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFileReaderWriterFactory.class);
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;
    protected AvroSchemaRegistry schemaRegistry;

    public AvroParquetFileReaderWriterFactory(SecorConfig config) {
        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
        schemaRegistry = AvroSchemaRegistryFactory.getSchemaRegistry(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileWriter(logFilePath, codec);
    }

    protected class AvroParquetFileReader implements FileReader {

        private ParquetReader<GenericRecord> reader;
        private SpecificDatumWriter<GenericRecord> writer;
        private long offset;
        String topic;


        public AvroParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            topic = logFilePath.getTopic();
            Schema schema = schemaRegistry.getSchema(topic);
            reader = AvroParquetReader.<GenericRecord>builder(path).build();
            writer = new SpecificDatumWriter(schema);
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            GenericRecord record = reader.read();
            if (record != null) {
                return new KeyValue(offset++, schemaRegistry.serialize(topic, record));
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

    protected class AvroParquetFileWriter implements FileWriter {

        private ParquetWriter writer;
        private String topic;
        private Schema schema;
        private final Map<Schema, SpecificDatumReader<GenericRecord>> resolvingReaders = new HashMap<>();

        public AvroParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            LOG.debug("Creating Brand new Writer for path {}", path);
            CompressionCodecName codecName = CompressionCodecName
                    .fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            schema = schemaRegistry.getSchema(topic);
            // Not setting blockSize, pageSize, enableDictionary, and validating
            writer = AvroParquetWriter.builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(codecName)
                    .build();
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            GenericRecord record;
            try {
                record = toFileSchema(schemaRegistry.deserialize(topic, keyValue.getValue()));
            } catch (Exception ex) {
                // Skip records that cannot be read as the file schema (corrupt payloads or
                // incompatible schema changes) rather than blocking the partition on them.
                LOG.warn("Skipped record at offset {} of topic {}: cannot read it as schema {}",
                        keyValue.getOffset(), topic, schema.getFullName(), ex);
                return;
            }
            LOG.trace("Writing record {}", record);
            if (record != null) {
                writer.write(record);
            }
        }

        /**
         * AvroWriteSupport reads record fields by position in the file schema, so a record
         * deserialized with any other schema version (e.g. from a producer on an older or newer
         * schema) must first be resolved to the file schema using Avro schema evolution rules.
         */
        private GenericRecord toFileSchema(GenericRecord record) throws IOException {
            if (record == null || record.getSchema().equals(schema)) {
                return record;
            }
            Schema recordSchema = record.getSchema();
            byte[] payload = AvroSerializer.serialize(new SpecificDatumWriter<>(recordSchema), record);
            SpecificDatumReader<GenericRecord> reader = resolvingReaders.computeIfAbsent(recordSchema,
                    writerSchema -> new SpecificDatumReader<>(writerSchema, schema));
            return reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}

