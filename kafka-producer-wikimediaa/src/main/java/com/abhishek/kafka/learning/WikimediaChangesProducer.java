package com.abhishek.kafka.learning;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootsrapserver="127.0.0.1:9092";

        //create producer properties

        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootsrapserver);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //advanced properties
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        p.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        p.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        KafkaProducer<String , String > producer =new KafkaProducer<>(p);

        String topic = "wikimedia.recentchanges";
        EventHandler eventhandler=new WikimediaChangeHandler(producer,topic);



        String url= "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder=new EventSource.Builder(eventhandler, URI.create(url));
        builder.headers(Headers.of(Map.of(
                "User-Agent",
                "KafkaWikimediaProducer/1.0 (abhishekdwivedi9444@gmail.com)"
        )));
        EventSource eventSource=builder.build();


        //startproducer in another thread

            eventSource.start();

//      we produce for 10 minutes and then block the code
        TimeUnit.MINUTES.sleep(10);



    }
}