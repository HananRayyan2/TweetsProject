package com.finaly.spark;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) throws IOException , InterruptedException {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:9092";
        File file=new File("C:\\\\tweetsData\\\\tweet.json");    //creates a new file instance
        FileReader fr=new FileReader(file);   //reads the file
        BufferedReader br=new BufferedReader(fr);
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String line;
        ProducerRecord<String, String>producerRecord = null;
        while((line=br.readLine())!=null)
        {

          //  System.out.println("sleep");
            // create a producer record
             producerRecord =
                    new ProducerRecord<>("test1", line);
            // send data - asynchronous
            producer.send(producerRecord);
            System.out.println(line);
            Thread.sleep(1000);
            producer.flush();
        }



        

        // flush data - synchronous

        // flush and close producer
        producer.close();
    }
}
