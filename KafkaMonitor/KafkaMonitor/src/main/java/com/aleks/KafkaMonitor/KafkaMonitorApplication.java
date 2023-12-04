package com.aleks.KafkaMonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@SpringBootApplication
public class KafkaMonitorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaMonitorApplication.class, args);
	}

}
