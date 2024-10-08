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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.ParquetUtil;
import com.pinterest.secor.util.ProtobufUtil;

/**
 * Implementation for reading/writing protobuf messages to/from Parquet files.
 * 
 * @author Michael Spector (spektom@gmail.com)
 */
public class ProtobufParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private ProtobufUtil protobufUtil;

    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;

    public ProtobufParquetFileReaderWriterFactory(SecorConfig config) {
        protobufUtil = new ProtobufUtil(config);

        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new ProtobufParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new ProtobufParquetFileWriter(logFilePath, codec);
    }

    protected class ProtobufParquetFileReader implements FileReader {

        private ParquetReader<MessageOrBuilder> reader;
        private long offset;

        public ProtobufParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            reader = ProtoParquetReader.<MessageOrBuilder>builder(path).build();
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            Builder messageBuilder = (Builder) reader.read();
            if (messageBuilder != null) {
                return new KeyValue(offset++, messageBuilder.build().toByteArray());
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    protected class ProtobufParquetFileWriter implements FileWriter {

        private WriteCompliantProtoParquetWriter<Message> writer;
        private String topic;

        public ProtobufParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            CompressionCodecName codecName = CompressionCodecName
                    .fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            // using write compliant parquet writer
            writer = new WriteCompliantProtoParquetWriter<Message>(path, protobufUtil.getMessageClass(topic), codecName,
                    blockSize, pageSize, enableDictionary, validating);
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            Message message = protobufUtil.decodeProtobufOrJsonMessage(topic, keyValue.getValue());
            writer.write(message);
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
