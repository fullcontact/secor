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
    // Schema Registry REST API error code for an unknown subject.
    private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

    protected KafkaAvroDeserializer deserializer;
    protected KafkaAvroSerializer serializer;
    // Static so every instance shares them: a new client is built for each file writer, and the
    // parser's deserialize calls are what reveal newly registered schema versions.
    // Newest known schema of each topic; used as the schema of newly created files.
    private final static Map<String, CachedSchema> schemas = new ConcurrentHashMap<>();
    // Schema versions already seen in records of each topic. A schema is added only after the
    // cache entry its refresh produced is published, so any writer created for one of its
    // records finds a cached schema.
    private final static Map<String, Set<Schema>> seenSchemas = new ConcurrentHashMap<>();
    // Serializes each topic's cache refreshes and initialization across the consumer threads.
    private final static Map<String, Object> topicLocks = new ConcurrentHashMap<>();
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
        if (record != null && !seenSchemas(topic).contains(record.getSchema())) {
            refreshLatestSchema(topic, record.getSchema());
        }
        return record;
    }

    private static Set<Schema> seenSchemas(String topic) {
        return seenSchemas.computeIfAbsent(topic, t -> ConcurrentHashMap.newKeySet());
    }

    private static Object topicLock(String topic) {
        return topicLocks.computeIfAbsent(topic, t -> new Object());
    }

    // A schema version not seen before may have been registered since the cached latest schema
    // was fetched. Records on older versions do not replace it, so files keep the newest fields.
    private void refreshLatestSchema(String topic, Schema recordSchema) {
        synchronized (topicLock(topic)) {
            Set<Schema> seen = seenSchemas(topic);
            if (seen.contains(recordSchema)) {
                return;
            }
            CachedSchema latest;
            try {
                latest = fetchLatestSchema(topic);
            } catch (IllegalStateException e) {
                // Topics whose schema is not registered under "<topic>-value" can only be written
                // with a schema taken from their records. Any other failure is transient, and
                // publishing the record's schema cannot replace a cached registered one.
                if (!isSubjectNotFound(e.getCause())) {
                    LOG.warn("Unable to refresh Avro schema for topic {}; keeping the cached registered schema, if any",
                            topic, e);
                }
                latest = CachedSchema.fromRecord(recordSchema);
            }
            schemas.merge(topic, latest, CachedSchema::newer);
            seen.add(recordSchema);
        }
    }

    private static boolean isSubjectNotFound(Throwable e) {
        return e instanceof RestClientException
                && ((RestClientException) e).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE;
    }

    /**
     * Get the latest Avro schema registered for a topic. It is cached, and refreshed by
     * {@link #deserialize(String, byte[])} whenever a record carries a schema version not seen before.
     * It uses standard "subject name" strategy and it is topic_name-value. For a topic without such
     * a subject, it is the schema most recently seen for the first time in the topic's records,
     * since without a subject there are no versions to order the schemas by.
     *
     * @param topic a Kafka topic to query the schema for
     * @return Schema object for the topic
     * @throws IllegalStateException if there is no schema registered for this topic or it is not able to fetch it
     */
    public Schema getSchema(String topic) {
        CachedSchema cached = schemas.get(topic);
        if (cached == null) {
            synchronized (topicLock(topic)) {
                cached = schemas.get(topic);
                if (cached == null) {
                    cached = fetchLatestSchema(topic);
                    schemas.put(topic, cached);
                }
            }
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
        // Version of schemas taken from records; registry subject versions start at 1.
        private static final int UNVERSIONED = 0;

        final Schema schema;
        // Subject version of a schema fetched from the registry.
        final int version;

        private CachedSchema(Schema schema, int version) {
            this.schema = schema;
            this.version = version;
        }

        static CachedSchema registered(Schema schema, int version) {
            return new CachedSchema(schema, version);
        }

        static CachedSchema fromRecord(Schema schema) {
            return new CachedSchema(schema, UNVERSIONED);
        }

        boolean isRegistered() {
            return version != UNVERSIONED;
        }

        // The registry's latest version of the subject always outranks a schema taken from a
        // record, so a registry outage does not replace it.
        static CachedSchema newer(CachedSchema cached, CachedSchema candidate) {
            if (cached.isRegistered() != candidate.isRegistered()) {
                return cached.isRegistered() ? cached : candidate;
            }
            if (!candidate.isRegistered()) {
                return candidate;
            }
            return candidate.version > cached.version ? candidate : cached;
        }
    }
}
