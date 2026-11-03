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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SecorSchemaRegistryClient implements AvroSchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SecorSchemaRegistryClient.class);
    // Schema Registry REST API error code for an unknown subject.
    private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

    protected KafkaAvroDeserializer deserializer;
    protected KafkaAvroSerializer serializer;
    // Static so every instance shares them: a new client is built for each file writer, and the
    // parser's deserialize calls are what reveal newly registered schema versions.
    // Newest known schema of each topic; used as the schema of newly created files.
    private final static Map<String, CachedSchema> schemas = new ConcurrentHashMap<>();
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
            refreshLatestSchema(topic, record.getSchema(), schemaId(message));
        }
        return record;
    }

    // Confluent wire format: a zero magic byte, then the 4-byte big-endian schema ID. The
    // deserializer has already validated the header of any message it decoded.
    private static int schemaId(byte[] message) {
        return ByteBuffer.wrap(message, 1, 4).getInt();
    }

    // A schema version not seen before may have been registered since the cached latest schema
    // was fetched. Records on older versions do not replace it, so files keep the newest fields.
    private void refreshLatestSchema(String topic, Schema recordSchema, int recordSchemaId) {
        CachedSchema latest;
        try {
            latest = fetchLatestSchema(topic);
        } catch (IllegalStateException e) {
            // Topics whose schema is not registered under "<topic>-value" can only be written
            // with a schema taken from their records. Any other failure is transient, and
            // publishing the record's schema cannot replace a cached registered one.
            if (!isSubjectNotFound(e.getCause())) {
                LOG.warn("Unable to refresh Avro schema for topic {}; keeping the cached registered schema, if any", topic, e);
            }
            latest = CachedSchema.fromRecord(recordSchema, recordSchemaId);
        }
        // Refreshes run concurrently on the consumer threads; merging by recency keeps a slow
        // refresh that fetched an older version from overwriting a newer one.
        schemas.merge(topic, latest, CachedSchema::newer);
    }

    private static boolean isSubjectNotFound(Throwable e) {
        return e instanceof RestClientException
                && ((RestClientException) e).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE;
    }

    /**
     * Get the latest Avro schema registered for a topic. It is cached, and refreshed by
     * {@link #deserialize(String, byte[])} whenever a record carries a schema version not seen before.
     * It uses standard "subject name" strategy and it is topic_name-value. For a topic without such
     * a subject, it is the newest schema seen in the topic's records.
     *
     * @param topic a Kafka topic to query the schema for
     * @return Schema object for the topic
     * @throws IllegalStateException if there is no schema registered for this topic or it is not able to fetch it
     */
    public Schema getSchema(String topic) {
        CachedSchema cached = schemas.get(topic);
        if (cached == null) {
            cached = schemas.merge(topic, fetchLatestSchema(topic), CachedSchema::newer);
        }
        return cached.schema;
    }

    private CachedSchema fetchLatestSchema(String topic) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value");
            return CachedSchema.registered(schemaRegistryClient.getByID(schemaMetadata.getId()),
                    schemaMetadata.getVersion());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get Avro schema not found for topic " + topic, e);
        } catch (RestClientException e) {
            throw new IllegalStateException("Avro schema not found for topic " + topic, e);
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) throws IOException {
        return serializer.serialize(topic, record);

    }

    private static final class CachedSchema {
        final Schema schema;
        final boolean registered;
        // Registered schemas are ordered by subject version; schemas taken from records by
        // schema ID, which the registry assigns in increasing order as schemas are registered.
        final int recency;

        private CachedSchema(Schema schema, boolean registered, int recency) {
            this.schema = schema;
            this.registered = registered;
            this.recency = recency;
        }

        static CachedSchema registered(Schema schema, int version) {
            return new CachedSchema(schema, true, version);
        }

        static CachedSchema fromRecord(Schema schema, int schemaId) {
            return new CachedSchema(schema, false, schemaId);
        }

        // The registry's latest version of the subject always outranks a schema taken from a record.
        static CachedSchema newer(CachedSchema cached, CachedSchema candidate) {
            if (cached.registered != candidate.registered) {
                return cached.registered ? cached : candidate;
            }
            return candidate.recency > cached.recency ? candidate : cached;
        }
    }
}
