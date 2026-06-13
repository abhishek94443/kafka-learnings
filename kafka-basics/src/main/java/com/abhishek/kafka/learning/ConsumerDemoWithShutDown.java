package com.abhishek.kafka.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    public static final Logger log= LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
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
//get a refrence of current thread'
      final  Thread t =Thread.currentThread();

      //adding a shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("detected shutdown lets exit by calling consumer.wakeup");
                consumer.wakeup();
                //above will throw always exception so we will catch it and in finally close the consumer so offset wil comiit at the point we stop
             //this will trigger exception in .poll method
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });



    try {
        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));
        // poll the data

        while (true) {
            log.info("polling data");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("key " + record.key() + " value " + record.value());
                log.info("key " + record.partition() + " value " + record.offset());
            }
        }


    }catch(WakeupException e){
        log.info("Consumer is getting shutdown");
    }
    catch (Exception e){
        log.info("Unexpected Exception"+ e);
    }
    finally {
        consumer.close();
        log.info("consumer gracefully shutdown");
    }
    }
}
