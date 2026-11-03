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

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroSerializer;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class AvroFileReaderWriterFactoryTest extends TestCase {

    private AvroFileReaderWriterFactory mFactory;
    private SpecificDatumWriter<GenericRecord> writer;
    private SecorConfig config;
    private SecorSchemaRegistryClient secorSchemaRegistryClient;
    private GenericRecord msg1;
    private GenericRecord msg2;

    @Override
    public void setUp() throws Exception {

        Schema schema = SchemaBuilder.record("UnitTestRecord")
                .fields()
                .name("data").type().stringType().noDefault()
                .name("timestamp").type().nullable().longType().noDefault()
                .endRecord();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        msg1 = builder.set("data", "foo").set("timestamp", 1467176315L).build();
        msg2 = builder.set("data", "bar").set("timestamp", 1467176344L).build();

        writer = new SpecificDatumWriter(schema);

        config = Mockito.mock(SecorConfig.class);
        when(config.getSchemaRegistryUrl()).thenReturn("test");
        secorSchemaRegistryClient = Mockito.mock(SecorSchemaRegistryClient.class);
        when(secorSchemaRegistryClient.getSchema(anyString())).thenReturn(schema);
        mFactory = new AvroFileReaderWriterFactory(config);
        when(secorSchemaRegistryClient.deserialize("test-avro-topic", AvroSerializer.serialize(writer, msg1))).thenReturn(msg1);
        when(secorSchemaRegistryClient.deserialize("test-avro-topic", AvroSerializer.serialize(writer, msg2))).thenReturn(msg2);
        mFactory.schemaRegistry = secorSchemaRegistryClient;

    }

    @Test
    public void testAvroReadWriteRoundTrip() throws Exception {
        when(config.getFileReaderWriterFactory())
                .thenReturn(AvroFileReaderWriterFactory.class.getName());

        String topic = "test-avro-topic";

        when(secorSchemaRegistryClient.serialize(anyString(), Matchers.any(GenericRecord.class))).
                thenReturn(AvroSerializer.serialize(writer, msg1), AvroSerializer.serialize(writer, msg2));


        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), topic,
                new String[]{"part-1"}, 0, 1, 23232, ".parquet");

        FileWriter fileWriter = mFactory.BuildFileWriter(tempLogFilePath, null);

        KeyValue kv1 = new KeyValue(23232, AvroSerializer.serialize(writer, msg1));
        KeyValue kv2 = new KeyValue(23233, AvroSerializer.serialize(writer, msg2));

        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = mFactory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        assertEquals(msg1, secorSchemaRegistryClient.deserialize("test-avro-topic", kvout.getValue()));

        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        assertEquals(msg2, secorSchemaRegistryClient.deserialize("test-avro-topic", kvout.getValue()));
    }


    private static final Schema SCHEMA_V1 = SchemaBuilder.record("UnitTestRecord")
            .fields()
            .name("data").type().stringType().noDefault()
            .name("timestamp").type().nullable().longType().noDefault()
            .endRecord();
    // Adds a field ahead of "timestamp", so field positions differ from SCHEMA_V1.
    private static final Schema SCHEMA_V2 = SchemaBuilder.record("UnitTestRecord")
            .fields()
            .name("data").type().stringType().noDefault()
            .name("source").type().stringType().stringDefault("unknown")
            .name("timestamp").type().nullable().longType().noDefault()
            .endRecord();

    private List<GenericRecord> writeAndReadBack(GenericRecord... records) throws Exception {
        String topic = "test-avro-topic";
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), topic,
                new String[]{"part-1"}, 0, 1, 23232, ".avro");
        FileWriter fileWriter = mFactory.BuildFileWriter(tempLogFilePath, null);
        long offset = 23232;
        for (GenericRecord record : records) {
            byte[] payload = AvroSerializer.serialize(new SpecificDatumWriter<>(record.getSchema()), record);
            when(secorSchemaRegistryClient.deserialize(topic, payload)).thenReturn(record);
            fileWriter.write(new KeyValue(offset++, payload));
        }
        fileWriter.close();

        List<GenericRecord> written = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new File(tempLogFilePath.getLogFilePath()), new GenericDatumReader<>())) {
            reader.forEach(written::add);
        }
        return written;
    }

    @Test
    public void testWriteResolvesRecordsToFirstRecordSchema() throws Exception {
        GenericRecord newRecord = new GenericRecordBuilder(SCHEMA_V2)
                .set("data", "bar").set("source", "web").set("timestamp", 1467176344L).build();
        GenericRecord oldRecord = new GenericRecordBuilder(SCHEMA_V1)
                .set("data", "foo").set("timestamp", 1467176315L).build();

        List<GenericRecord> written = writeAndReadBack(newRecord, oldRecord);

        assertEquals(2, written.size());
        assertEquals(SCHEMA_V2, written.get(0).getSchema());
        assertEquals("bar", written.get(0).get("data").toString());
        assertEquals("web", written.get(0).get("source").toString());
        assertEquals(1467176344L, written.get(0).get("timestamp"));
        assertEquals("foo", written.get(1).get("data").toString());
        assertEquals("unknown", written.get(1).get("source").toString());
        assertEquals(1467176315L, written.get(1).get("timestamp"));
    }

    @Test
    public void testWriteKeepsRecordsOlderThanLatestRegisteredSchema() throws Exception {
        // The latest registered schema adds a field without a default, so v1 records cannot be
        // resolved to it.
        Schema requiresSource = SchemaBuilder.record("UnitTestRecord")
                .fields()
                .name("data").type().stringType().noDefault()
                .name("source").type().stringType().noDefault()
                .name("timestamp").type().nullable().longType().noDefault()
                .endRecord();
        when(secorSchemaRegistryClient.getSchema(anyString())).thenReturn(requiresSource);
        GenericRecord oldRecord = new GenericRecordBuilder(SCHEMA_V1)
                .set("data", "foo").set("timestamp", 1467176315L).build();

        List<GenericRecord> written = writeAndReadBack(oldRecord);

        assertEquals(1, written.size());
        assertEquals(SCHEMA_V1, written.get(0).getSchema());
        assertEquals("foo", written.get(0).get("data").toString());
    }
}
