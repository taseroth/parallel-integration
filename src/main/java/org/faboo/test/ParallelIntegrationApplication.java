package org.faboo.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.Channels;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.endpoint.MethodInvokingMessageSource;
import org.springframework.messaging.Message;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class ParallelIntegrationApplication {



    @Bean
    public MessageSource<?> integerMessageSource() {
        MethodInvokingMessageSource source = new MethodInvokingMessageSource();
        source.setObject(new AtomicInteger());
        source.setMethodName("getAndIncrement");
        return source;
    }

    private void logMessage(Message message) {
        System.out.println(message);
    }


    @Bean
    public IntegrationFlow integrationFlow() {
        return IntegrationFlows
                .from(integerMessageSource(), c -> c.poller(Pollers.fixedRate(1, TimeUnit.SECONDS)))
                .channel(Channels::direct)
                .handle(this::logMessage)
                .get();

    }

	public static void main(String[] args) {
		SpringApplication.run(ParallelIntegrationApplication.class, args);
	}
}
