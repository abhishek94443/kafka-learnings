package com.abhishek.kafka.learning;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerDemo {
    public static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {

      log.info("Hello world!");


      //Create producer properties
        Properties p=new Properties();
        p.setProperty("bootstrap.servers","127.0.0.1:9092");

      //set the producer property
        p.setProperty("key.serializer", StringSerializer.class.getName());
        p.setProperty("value.serializer", StringSerializer.class.getName());
      // Create the producer
        KafkaProducer<String ,String> producer=new KafkaProducer<>(p);

      //send data
        ProducerRecord <String,String> record=new ProducerRecord<>("second_topic","hello world");

      // flush  the producer
       producer.flush();

       // flush and close the producer

        producer.close();
    }
}