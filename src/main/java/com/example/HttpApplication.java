package com.example;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.PreDestroy;

import io.projectriff.invoker.FunctionProperties;
import io.projectriff.invoker.GrpcConfiguration;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

@SpringBootApplication
@Import(GrpcConfiguration.class)
@EnableConfigurationProperties(FunctionProperties.class)
@RestController
public class HttpApplication {

	private EmitterProcessor<Message<Object>> processor = EmitterProcessor.create();

	@PostMapping
	public void home(@RequestBody byte[] payload, @RequestHeader HttpHeaders headers) {
		processor.onNext(MessageBuilder.createMessage(payload, fromHttp(headers)));
	}

	@Bean
	public Supplier<Flux<Message<Object>>> relay() {
		return () -> processor.log();
	}

	@PreDestroy
	public void close() {
		processor.onComplete();
	}

	public static void main(String[] args) {
		new SpringApplicationBuilder(HttpApplication.class).run(args)
				.getBean(GrpcConfiguration.class).awaitTermination();
	}

	public static MessageHeaders fromHttp(HttpHeaders headers) {
		Map<String, Object> map = new LinkedHashMap<>();
		for (String name : headers.keySet()) {
			Collection<?> values = multi(headers.get(name));
			name = name.toLowerCase();
			Object value = values == null ? null
					: (values.size() == 1 ? values.iterator().next() : values);
			map.put(name, value);
		}
		return new MessageHeaders(map);
	}

	private static Collection<?> multi(Object value) {
		if (value instanceof Collection) {
			Collection<?> collection = (Collection<?>) value;
			return collection;
		}
		else if (ObjectUtils.isArray(value)) {
			Object[] values = ObjectUtils.toObjectArray(value);
			return Arrays.asList(values);
		}
		return Arrays.asList(value);
	}

}
