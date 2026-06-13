package com.abhishek.kafka.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger log= LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("I am consumer");
        String group = "my-java-application";
String topic ="second_topic";

        //Create producer properties
        Properties p=new Properties();
        p.setProperty("bootstrap.servers","172.23.231.67:9092");

        //set the producer property
        p.setProperty("key.deserializer", StringDeserializer.class.getName());
        p.setProperty("value.deserializer", StringDeserializer.class.getName());
        p.setProperty("group.id",group);
        p.setProperty("auto.offset.reset","earliest");
       /*"none/earliest/latest" Values
       * "none if we dont have any consumer group  then we fail we must se consumer grp
       /earliest read from the beggining of my topic
       /latest" only read new msg sends from now

       *  */
        // Create the consumer
        KafkaConsumer<String , String > consumer=new KafkaConsumer<>(p);

        //subscribe to a topic
consumer.subscribe(Arrays.asList(topic));
        // poll the data

        while(true){
            log.info("polling data");
          ConsumerRecords<String ,String > records = consumer.poll(Duration.ofMillis(1000));

          for(ConsumerRecord<String,String> record : records){
              log.info("key "+ record.key() + " value "+ record.value());
              log.info("key "+ record.partition() + " value "+ record.offset());
          }
        }




    }
}
