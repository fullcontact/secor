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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SecorSchemaRegistryClient implements AvroSchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SecorSchemaRegistryClient.class);

    protected KafkaAvroDeserializer deserializer;
    protected KafkaAvroSerializer serializer;
    // Static so every instance shares them: a new client is built for each file writer, and the
    // parser's deserialize calls are what reveal newly registered schema versions.
    // Latest schema registered for each topic; used as the schema of newly created files.
    private final static Map<String, Schema> schemas = new ConcurrentHashMap<>();
    // Schema versions already seen in records of each topic.
    private final static Map<String, Set<Schema>> seenSchemas = new ConcurrentHashMap<>();
    protected SchemaRegistryClient schemaRegistryClient;

    public SecorSchemaRegistryClient(SecorConfig config) {
        try {
            Properties props = new Properties();
            props.put("schema.registry.url", config.getSchemaRegistryUrl());
            schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrl(), 30);
            init(config);
        } catch (Exception e) {
            LOG.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }
    }

    //Allows the SchemaRegistryClient to be mocked in unit tests
    protected void init(SecorConfig config) {
        deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        serializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    public GenericRecord deserialize(String topic, byte[] message) {
        if (message.length == 0) {
            message = null;
        }
        GenericRecord record = (GenericRecord) deserializer.deserialize(topic, message);
        if (record != null && seenSchemas.computeIfAbsent(topic, t -> ConcurrentHashMap.newKeySet())
                .add(record.getSchema())) {
            refreshLatestSchema(topic, record.getSchema());
        }
        return record;
    }

    // A schema version not seen before may have been registered since the cached latest schema
    // was fetched. Records on older versions do not replace it, so files keep the newest fields.
    private void refreshLatestSchema(String topic, Schema recordSchema) {
        try {
            schemas.put(topic, fetchLatestSchema(topic));
        } catch (IllegalStateException e) {
            // Topics whose schema is not registered under "<topic>-value" can only be written
            // with a schema taken from their records.
            LOG.warn("Unable to refresh Avro schema for topic {}; keeping the cached schema", topic, e);
            schemas.putIfAbsent(topic, recordSchema);
        }
    }

    /**
     * Get the latest Avro schema registered for a topic. It is cached, and refreshed by
     * {@link #deserialize(String, byte[])} whenever a record carries a schema version not seen before.
     * It uses standard "subject name" strategy and it is topic_name-value.
     *
     * @param topic a Kafka topic to query the schema for
     * @return Schema object for the topic
     * @throws IllegalStateException if there is no schema registered for this topic or it is not able to fetch it
     */
    public Schema getSchema(String topic) {
        Schema schema = schemas.get(topic);
        if (schema == null) {
            schema = fetchLatestSchema(topic);
            schemas.put(topic, schema);
        }
        return schema;
    }

    private Schema fetchLatestSchema(String topic) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value");
            return schemaRegistryClient.getByID(schemaMetadata.getId());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get Avro schema not found for topic " + topic);
        } catch (RestClientException e) {
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) throws IOException {
        return serializer.serialize(topic, record);

    }
}
