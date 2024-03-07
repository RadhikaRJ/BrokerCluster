package com.example.BrokerNodeServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BrokerNodeServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(BrokerNodeServerApplication.class, args);
	}

}
