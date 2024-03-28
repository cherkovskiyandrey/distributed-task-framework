package com.distributed_task_framework.test_service.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springdoc.core.customizers.SpringDocCustomizers;
import org.springdoc.core.properties.SpringDocConfigProperties;
import org.springdoc.core.providers.SpringDocProviders;
import org.springdoc.core.service.AbstractRequestService;
import org.springdoc.core.service.GenericResponseService;
import org.springdoc.core.service.OpenAPIService;
import org.springdoc.core.service.OperationService;
import org.springdoc.core.utils.Constants;
import org.springdoc.webmvc.api.OpenApiWebMvcResource;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.replaceOnceIgnoreCase;

@Slf4j
@Configuration
@OpenAPIDefinition(
        info = @Info(title = "Test application API", version = "v1")
)
public class SwaggerConfig {
    @Bean
    @ConditionalOnProperty(name = Constants.SPRINGDOC_USE_MANAGEMENT_PORT, havingValue = "false", matchIfMissing = true)
    OpenApiWebMvcResource openApiResource(ObjectFactory<OpenAPIService> openAPIBuilderObjectFactory,
                                          AbstractRequestService requestBuilder,
                                          GenericResponseService responseBuilder,
                                          OperationService operationParser,
                                          SpringDocConfigProperties springDocConfigProperties,
                                          SpringDocProviders springDocProviders,
                                          SpringDocCustomizers springDocCustomizers) {
        return new OpenApiWebMvcResource(
                openAPIBuilderObjectFactory,
                requestBuilder,
                responseBuilder,
                operationParser,
                springDocConfigProperties,
                springDocProviders,
                springDocCustomizers) {

            @Override
            protected String getServerUrl(HttpServletRequest request, String apiDocsUrl) {
                String serverUrl = super.getServerUrl(request, apiDocsUrl);
                if (log.isDebugEnabled()) {
                    String allHeaders = Collections.list(request.getHeaderNames()).stream()
                            .map(key -> key + ": " + request.getHeader(key))
                            .collect(Collectors.joining(" || "));
                    log.info("HttpServletRequest headers: {}", allHeaders);
                }

                if (serverUrl.toLowerCase().startsWith("https")) {
                    return serverUrl;
                }

                if (equalsIgnoreCase("https", request.getHeader("x-forwarded-proto"))) {
                    return replaceOnceIgnoreCase(serverUrl, "http", "https");
                }

                if (StringUtils.hasText(request.getHeader("forwarded"))) {
                    Map<String, String> forwarded = Arrays.stream(
                                    Optional.ofNullable(
                                                    request.getHeader("forwarded"))
                                            .orElse("")
                                            .split(";")
                            )
                            .map(keyVal -> Pair.of(keyVal.split("=")[0], keyVal.split("=")[1]))
                            .collect(Collectors.toMap(
                                    Pair::getKey,
                                    Pair::getValue
                            ));
                    if ("https".equalsIgnoreCase(forwarded.get("proto"))) {
                        return replaceOnceIgnoreCase(serverUrl, "http", "https");
                    }
                }

                return serverUrl;
            }
        };
    }
}
