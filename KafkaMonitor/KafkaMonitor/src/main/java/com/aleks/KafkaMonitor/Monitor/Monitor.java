package com.aleks.KafkaMonitor.Monitor;

import com.aleks.KafkaMonitor.Runnable.LagAnalyzerThread;
import com.aleks.KafkaMonitor.config.OffSetConsumer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class Monitor {

    OffSetConsumer consumer;

    String bootstrapServer;
    String topic;
    String consumerGroupId;
    List<LagAnalyzerThread> listaThreads;

    public Monitor(){
        bootstrapServer = "localhost:9092";
        topic = "teste-aleks";
        consumerGroupId = "MeuGroupId";
        listaThreads = new ArrayList<LagAnalyzerThread>();
    }

    @Scheduled(fixedRate = 5000)
    public void monitorar(){

        if(listaThreads.size() == 0){
            LagAnalyzerThread t = new LagAnalyzerThread(topic, bootstrapServer, consumerGroupId);
            listaThreads.add(t);
        }

        for(LagAnalyzerThread thread : listaThreads){
            if(thread.getTopic().equals(topic)){
                if(!thread.isAlive()){
                    thread.run();
                }
            }
        }
    }
}
