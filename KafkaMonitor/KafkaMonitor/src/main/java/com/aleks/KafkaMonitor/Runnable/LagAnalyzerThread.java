package com.aleks.KafkaMonitor.Runnable;

import com.aleks.KafkaMonitor.config.OffSetConsumer;

import java.util.concurrent.ExecutionException;

public class LagAnalyzerThread implements Runnable {

    private String topic;
    private String bootstrapServer;
    private OffSetConsumer consumer;
    private String consumerGroupId;
    private boolean running = false;

    public LagAnalyzerThread(String topic, String bootstrapServer, String consumerGroupId){
        this.topic = topic;
        this.bootstrapServer = bootstrapServer;
        this.consumerGroupId = consumerGroupId;
        consumer = new OffSetConsumer(this.bootstrapServer, this.topic, consumerGroupId);
    }

    public String getTopic(){
        return topic;
    }

    public void setTopic(String topic){
        this.topic = topic;
    }

    public String getBootstrapServer(){
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer){
        this.bootstrapServer = bootstrapServer;
    }

    public boolean isAlive(){
        return running;
    }

    @Override
    public void run() {
        try {
            while(true){
                running = true;
                consumer.analyzeLag();
                notify();
                Thread.sleep(1000);
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch(Exception e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

    }



}
