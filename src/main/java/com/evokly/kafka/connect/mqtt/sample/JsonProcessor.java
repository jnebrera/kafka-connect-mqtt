package com.evokly.kafka.connect.mqtt.sample;

import com.evokly.kafka.connect.mqtt.MqttMessageProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;


/**
 * Copyright 2016 Evokly S.A.
 *
 * <p>See LICENSE file for License
 **/
public class JsonProcessor implements MqttMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(JsonProcessor.class);
    private MqttMessage mMessage;
    private Object mTopic;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public MqttMessageProcessor process(String topic, MqttMessage message) {
        log.debug("processing data for topic: {}; with message {}", topic, message);
        this.mTopic = topic;
        this.mMessage = message;
        return this;
    }

    @Override
    public SourceRecord[] getRecords(String kafkaTopic) {
        SourceRecord[] records;
        try {
            Map<String, Object> value = mapper.readValue(mMessage.getPayload(), Map.class);
            records = new SourceRecord[]{new SourceRecord(null, null, kafkaTopic, null,
                    Schema.STRING_SCHEMA, mTopic,
                    Schema.STRING_SCHEMA, value)};
        } catch (IOException e) {
            records = new SourceRecord[]{};
            log.error(e.getMessage(), e);
        }

        return records;
    }
}
