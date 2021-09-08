package com.ygmpkk.marketdata.agent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
@EnableScheduling
public class MarketdataAgentApplication {
  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(MarketdataAgentApplication.class);
    app.setWebApplicationType(WebApplicationType.SERVLET);
    app.run(args);
  }
}
