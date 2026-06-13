package com.abhishek.kafka.learning;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {
    public static final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args)  {

        log.info("Hello world!");


        //Create producer properties
        Properties p=new Properties();
        p.setProperty("bootstrap.servers","172.23.231.67:9092");

        //set the producer property
        p.setProperty("key.serializer", StringSerializer.class.getName());
        p.setProperty("value.serializer", StringSerializer.class.getName());
        p.setProperty("batch.size","400");
;        // Create the producer
        KafkaProducer<String ,String> producer=new KafkaProducer<>(p){};

        for (int j = 0; j < 10; j++) {


        for (int i = 0; i < 30; i++) {
            //send data
            ProducerRecord<String,String> record=new ProducerRecord<>("second_topic","Hello"+j+" "+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("Message successfully produced ");
                        System.out.println("Topic "+recordMetadata.topic());
                        System.out.println("Partition "+recordMetadata.partition());
                        System.out.println("Offset "+recordMetadata.offset());
                        System.out.println("Timestamp "+recordMetadata.timestamp());
                    }
                    else{
                        System.out.println("Failed producing to queue "+e);
                    }
                }
            });

        }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // flush  the producer
        producer.flush();

        // flush and close the producer

        producer.close();
    }
}