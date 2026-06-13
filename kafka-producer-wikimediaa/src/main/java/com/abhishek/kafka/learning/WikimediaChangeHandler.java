package com.abhishek.kafka.learning;

import com.launchdarkly.eventsource.EventHandler;
//import com.launchdarkly.eventsource.Logger;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String,String> producer;
    String topic;
    private  final Logger log =LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());
    public WikimediaChangeHandler(KafkaProducer producer,String topic){
        this.producer=producer;
        this.topic=topic;
    }

    @Override
    public void onOpen()  {
//nothing here
    }

    @Override
    public void onClosed()  {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
        log.info(messageEvent.getData());
        //asynchronous code
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s)  {
// nothing here
    }

    @Override
    public void onError(Throwable throwable) {
//
        log.error("Error in stream reading "+ throwable);
    }
}
