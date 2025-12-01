package com.example.bajaj_test1.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        return builder
                .exchangeStrategies(
                        ExchangeStrategies.builder().codecs(
                                configurer -> configurer.defaultCodecs()
                                        .maxInMemorySize(16 * 1024 * 1024)
                        ).build()
                )
                .build();
    }
}
