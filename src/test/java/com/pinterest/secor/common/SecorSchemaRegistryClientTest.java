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
package com.pinterest.secor.common;

import com.pinterest.secor.util.AvroSerializer;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class SecorSchemaRegistryClientTest extends TestCase {

    private KafkaAvroDeserializer kafkaAvroDeserializer;
    private SchemaRegistryClient schemaRegistryClient;
    private SecorSchemaRegistryClient secorSchemaRegistryClient;
    private KafkaAvroSerializer avroSerializer;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Override
    public void setUp() {
        initKafka();
        SecorConfig secorConfig = Mockito.mock(SecorConfig.class);
        when(secorConfig.getSchemaRegistryUrl()).thenReturn("schema-registry-url");
        secorSchemaRegistryClient = new SecorSchemaRegistryClient(secorConfig);
        secorSchemaRegistryClient.deserializer = kafkaAvroDeserializer;
        secorSchemaRegistryClient.schemaRegistryClient = schemaRegistryClient;
    }

    private void initKafka() {
        schemaRegistryClient = new MockSchemaRegistryClient();
        kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Test
    public void testDecodeMessage() {
        Schema schemaV1 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        //backward compatible schema change
        Schema schemaV2 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("data_field_2").type().stringType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        GenericRecord record1 = new GenericRecordBuilder(schemaV1)
                .set("data_field_1", 1)
                .set("timestamp", 1467176315L)
                .build();
        GenericRecord record2 = new GenericRecordBuilder(schemaV2)
                .set("data_field_1", 1)
                .set("data_field_2", "hello")
                .set("timestamp", 1467176316L)
                .build();
        GenericRecord output = secorSchemaRegistryClient.deserialize("test-avr-topic", avroSerializer.serialize("test-avr-topic", record1));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV1);
        assertEquals(output.get("data_field_1"), 1);
        assertEquals(output.get("timestamp"), 1467176315L);

        output = secorSchemaRegistryClient.deserialize("test-avr-topic", avroSerializer.serialize("test-avr-topic", record2));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV2);
        assertEquals(output.get("data_field_1"), 1);
        assertTrue(StringUtils.equals((output.get("data_field_2")).toString(), "hello"));
        assertEquals(output.get("timestamp"), 1467176316L);

        output = secorSchemaRegistryClient.deserialize("test-avr-topic", new byte[0]);
        assertNull(output);
    }

    @Test
    public void testGetSchemaReturnsLatestRegisteredSchemaRegardlessOfRecordOrder() {
        String topic = "test-avr-topic-4";
        Schema schemaV1 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        Schema schemaV2 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("data_field_2").type().stringType().stringDefault("")
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        byte[] payloadV1 = avroSerializer.serialize(topic, new GenericRecordBuilder(schemaV1)
                .set("data_field_1", 1)
                .set("timestamp", 1467176315L)
                .build());
        byte[] payloadV2 = avroSerializer.serialize(topic, new GenericRecordBuilder(schemaV2)
                .set("data_field_1", 2)
                .set("data_field_2", "hello")
                .set("timestamp", 1467176316L)
                .build());

        // A producer still on v1 after v2 is registered must not roll the file schema back.
        secorSchemaRegistryClient.deserialize(topic, payloadV2);
        secorSchemaRegistryClient.deserialize(topic, payloadV1);
        assertEquals(schemaV2, secorSchemaRegistryClient.getSchema(topic));
    }

    private static final Schema FOO_V1 = SchemaBuilder.record("Foo")
            .fields()
            .name("data_field_1").type().intType().noDefault()
            .name("timestamp").type().longType().noDefault()
            .endRecord();
    private static final Schema FOO_V2 = SchemaBuilder.record("Foo")
            .fields()
            .name("data_field_1").type().intType().noDefault()
            .name("data_field_2").type().stringType().stringDefault("")
            .name("timestamp").type().longType().noDefault()
            .endRecord();

    // Serializing under "<subject topic>-value" registers the schemas, v1 before v2.
    private byte[][] serializeV1AndV2(String subjectTopic) {
        return new byte[][]{
                avroSerializer.serialize(subjectTopic, new GenericRecordBuilder(FOO_V1)
                        .set("data_field_1", 1)
                        .set("timestamp", 1467176315L)
                        .build()),
                avroSerializer.serialize(subjectTopic, new GenericRecordBuilder(FOO_V2)
                        .set("data_field_1", 2)
                        .set("data_field_2", "hello")
                        .set("timestamp", 1467176316L)
                        .build())};
    }

    @Test
    public void testGetSchemaFollowsNewestRecordSchemaWithoutRegisteredSubject() {
        String topic = "test-avr-topic-5";
        byte[][] payloads = serializeV1AndV2("test-avr-topic-5-other");

        secorSchemaRegistryClient.deserialize(topic, payloads[0]);
        assertEquals(FOO_V1, secorSchemaRegistryClient.getSchema(topic));
        secorSchemaRegistryClient.deserialize(topic, payloads[1]);
        assertEquals(FOO_V2, secorSchemaRegistryClient.getSchema(topic));
    }

    // Confluent wire format: a zero magic byte, the 4-byte big-endian schema ID, then the record.
    private static byte[] confluentPayload(int schemaId, GenericRecord record) throws IOException {
        byte[] body = AvroSerializer.serialize(new SpecificDatumWriter<>(record.getSchema()), record);
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(schemaId).put(body).array();
    }

    @Test
    public void testGetSchemaFollowsNewestRecordSchemaDespiteCrossSubjectIdReuse() throws Exception {
        String topic = "test-avr-topic-6";
        // v2 is registered under another subject first, so it has a lower schema ID than v1.
        int idV2 = schemaRegistryClient.register("test-avr-topic-6-early-value", FOO_V2);
        int idV1 = schemaRegistryClient.register("test-avr-topic-6-late-value", FOO_V1);
        assertTrue(idV2 < idV1);
        byte[] payloadV1 = confluentPayload(idV1, new GenericRecordBuilder(FOO_V1)
                .set("data_field_1", 1)
                .set("timestamp", 1467176315L)
                .build());
        byte[] payloadV2 = confluentPayload(idV2, new GenericRecordBuilder(FOO_V2)
                .set("data_field_1", 2)
                .set("data_field_2", "hello")
                .set("timestamp", 1467176316L)
                .build());

        secorSchemaRegistryClient.deserialize(topic, payloadV1);
        secorSchemaRegistryClient.deserialize(topic, payloadV2);
        assertEquals(FOO_V2, secorSchemaRegistryClient.getSchema(topic));
    }

    @Test
    public void testConcurrentFirstRecordsFindCachedSchemaWithoutRegisteredSubject() throws Exception {
        String topic = "test-avr-topic-9";
        byte[] payload = serializeV1AndV2("test-avr-topic-9-other")[0];

        CountDownLatch fetching = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SchemaRegistryClient slowRegistry = Mockito.mock(SchemaRegistryClient.class);
        when(slowRegistry.getLatestSchemaMetadata(topic + "-value")).thenAnswer(invocation -> {
            fetching.countDown();
            release.await();
            throw new RestClientException("Subject not found", 404, 40401);
        });
        secorSchemaRegistryClient.schemaRegistryClient = slowRegistry;

        Thread first = new Thread(() -> secorSchemaRegistryClient.deserialize(topic, payload));
        first.start();
        assertTrue(fetching.await(5, TimeUnit.SECONDS));

        // A second consumer gets a record of the same schema while the first refresh is pending,
        // then creates a writer for it.
        AtomicReference<Object> secondResult = new AtomicReference<>();
        Thread second = new Thread(() -> {
            try {
                secorSchemaRegistryClient.deserialize(topic, payload);
                secondResult.set(secorSchemaRegistryClient.getSchema(topic));
            } catch (RuntimeException e) {
                secondResult.set(e);
            }
        });
        second.start();
        long deadline = System.currentTimeMillis() + 5000;
        while (second.isAlive() && second.getState() != Thread.State.BLOCKED
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        release.countDown();
        first.join(5000);
        second.join(5000);

        assertEquals(FOO_V1, secondResult.get());
    }

    @Test
    public void testStaleRefreshDoesNotReplaceNewerRegisteredSchema() throws Exception {
        String topic = "test-avr-topic-7";
        int idV1 = schemaRegistryClient.register(topic + "-value", FOO_V1);
        int idV2 = schemaRegistryClient.register(topic + "-value", FOO_V2);
        byte[][] payloads = serializeV1AndV2(topic);

        // Simulates a refresh that fetched v1 metadata but published after one that fetched v2.
        SchemaRegistryClient staleRegistry = Mockito.mock(SchemaRegistryClient.class);
        when(staleRegistry.getLatestSchemaMetadata(topic + "-value")).thenReturn(
                new SchemaMetadata(idV2, 2, FOO_V2.toString()),
                new SchemaMetadata(idV1, 1, FOO_V1.toString()));
        when(staleRegistry.getByID(idV1)).thenReturn(FOO_V1);
        when(staleRegistry.getByID(idV2)).thenReturn(FOO_V2);
        secorSchemaRegistryClient.schemaRegistryClient = staleRegistry;

        secorSchemaRegistryClient.deserialize(topic, payloads[0]);
        secorSchemaRegistryClient.deserialize(topic, payloads[1]);
        assertEquals(FOO_V2, secorSchemaRegistryClient.getSchema(topic));
    }

    @Test
    public void testRegistryOutageKeepsCachedRegisteredSchema() throws Exception {
        String topic = "test-avr-topic-8";
        schemaRegistryClient.register(topic + "-value", FOO_V1);
        assertEquals(FOO_V1, secorSchemaRegistryClient.getSchema(topic));
        byte[] payloadV2 = serializeV1AndV2("test-avr-topic-8-other")[1];

        SchemaRegistryClient unavailableRegistry = Mockito.mock(SchemaRegistryClient.class);
        when(unavailableRegistry.getLatestSchemaMetadata(topic + "-value"))
                .thenThrow(new IOException("connection refused"));
        secorSchemaRegistryClient.schemaRegistryClient = unavailableRegistry;

        secorSchemaRegistryClient.deserialize(topic, payloadV2);
        assertEquals(FOO_V1, secorSchemaRegistryClient.getSchema(topic));
    }

    @Test
    public void testGetSchema() throws IOException, RestClientException {
        Schema expectedSchema = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        schemaRegistryClient.register("test-avr-topic-2-value", expectedSchema);
        Schema schema = secorSchemaRegistryClient.getSchema("test-avr-topic-2");
        assertEquals(expectedSchema, schema);
    }

    @Test
    public void testGetSchemaDoesNotExist() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Avro schema not found for topic test-avr-topic-3");
        secorSchemaRegistryClient.getSchema("test-avr-topic-3");
    }
}