/*
 * Copyright (c) 2024 vitasystems GmbH.
 *
 * This file is part of project EHRbase
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehrbase.service.validation;

import com.google.common.net.HttpHeaders;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.nedap.archie.rm.datatypes.CodePhrase;
import com.nedap.archie.rm.support.identification.TerminologyId;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.ehrbase.api.exception.InternalServerException;
import org.ehrbase.openehr.sdk.validation.ConstraintViolation;
import org.ehrbase.openehr.sdk.validation.terminology.ExternalTerminologyValidation;
import org.ehrbase.openehr.sdk.validation.terminology.ExternalTerminologyValidationException;
import org.ehrbase.openehr.sdk.validation.terminology.TerminologyParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

/**
 * {@link ExternalTerminologyValidation} that supports FHIR terminology validation.
 */
public class FhirTerminologyValidation implements ExternalTerminologyValidation {

    static final String SUPPORTS_CODE_SYS_TEMPL = "%s/CodeSystem?_summary=true&url=%s";
    static final String SUPPORTS_VALUE_SET_TEMPL = "%s/ValueSet?_summary=true&url=%s";
    private static final String VALUESET_VALIDATE_URL_TPL = "%s/ValueSet/$validate-code?url=%s&code=%s&system=%s";
    private static final String CODESYSTEM_VALIDATE_URL_TPL = "%s/CodeSystem/$validate-code?url=%s&code=%s";

    public static final JsonPath VALIDATE_CODE_RESULT_JSON_PATH =
            JsonPath.compile("$.parameter[?(@.name==\"result\" && \"valueBoolean\" == true)].valueBoolean");
    public static final JsonPath VALIDATE_CODE_MESSAGE_JSON_PATH =
            JsonPath.compile("$.parameter[?(@.name==\"message\")].valueString");
    public static final JsonPath SUPPORTS_TOTAL_JSON_PATH = JsonPath.compile("$.total");

    private static final String ERR_SUPPORTS =
            "An error occurred while checking if FHIR terminology server supports the referenceSetUri: %s";

    public static final String FHIR_ACCEPT_HEADER = "application/fhir+json";

    private static final String[] ACCEPTED_FHIR_APIS = {"//fhir.hl7.org", "terminology://fhir.hl7.org", "//hl7.org/fhir"
    };

    private static final Logger LOG = LoggerFactory.getLogger(FhirTerminologyValidation.class);

    private final String baseUrl;
    private final boolean failOnError;
    private final WebClient webClientTemplate;

    public FhirTerminologyValidation(String baseUrl) {
        this(baseUrl, true);
    }

    public FhirTerminologyValidation(String baseUrl, boolean failOnError) {
        this(baseUrl, failOnError, WebClient.create());
    }

    public FhirTerminologyValidation(String baseUrl, boolean failOnError, WebClient webClient) {
        this.baseUrl = baseUrl;
        this.failOnError = failOnError;
        this.webClientTemplate = webClient;
    }

    static String extractUrl(String referenceSetUri) {
        if (referenceSetUri == null) {
            return null;
        }

        UriComponents uriComponents = UriComponentsBuilder.fromUriString("/?%s".formatted(referenceSetUri))
                .build();
        MultiValueMap<String, String> queryParams = uriComponents.getQueryParams();
        return queryParams.getFirst("url");
    }

    private WebClient buildRestClientCall(String urlString) {
        String uriValue = uriTagValue(urlString);
        // in the metrics call, uri in the uriTagValue function does not have the query parameters
        //XXX CDR-2273 this means no re-use?

        HttpClient client = HttpClient.create().metrics(true, _ -> uriValue).responseTimeout(Duration.ofSeconds(10));

        return webClientTemplate
                .mutate()
                .clientConnector(new ReactorClientHttpConnector(client))
                .defaultHeader(HttpHeaders.ACCEPT, FHIR_ACCEPT_HEADER)
                //XXX CDR-2273 what is this for?
                .baseUrl(urlString)
                .build();
    }

    /**
     * Include the url parameter in the metrics uri tag
     * @param uri
     * @return
     */
    static String uriTagValue(String uri) {
        UriComponents uc = UriComponentsBuilder.fromUriString(uri).build();
        String path = uc.getPath();
        uc.getQueryParams().keySet().removeIf(k ->!"url".equals(k));

        //XXX CDR-2273 what format do we expect?? /fhir/CodeSystem/$validate-code?url=http://snomed.info/sct
        String terminologyUrl = uc.getQueryParams().getFirst("url");
        return terminologyUrl == null ? path : path + "?url=" + terminologyUrl;
    }

    protected DocumentContext internalGet(String uri)
            throws WebClientException, ExternalTerminologyValidationException {
        // timeout is managed by web client
        return getAsMono(uri)
                .map(FhirTerminologyValidation::toJsonDocument)
                .blockOptional()
                //XXX CDR-2273 In case the Mono errors, the original exception is thrown (wrapped in a RuntimeException if it was a checked exception)
                .orElseThrow(() ->new InternalServerException("could not connect to external Terminology Server"));
    }

    private static DocumentContext toJsonDocument(ResponseEntity<String> respEntity) {
        if (respEntity == null) {
            return null;
        }

        HttpStatusCode statusCode = respEntity.getStatusCode();
        if (statusCode.is2xxSuccessful()) {
            return JsonPath.parse(respEntity.getBody());
        } else {
            throw new ExternalTerminologyValidationException(
                    "Error response received from the terminology server. HTTP status: %s. Body: %s"
                            .formatted(statusCode, respEntity.getBody()));
        }
    }

    private static final class InternalRetryableException extends RuntimeException{}

    private Mono<ResponseEntity<String>> getAsMono(String uri) {
        WebClient client = buildRestClientCall(uri);
        return client.get()
                .uri(uri)
                .retrieve()
                .onStatus(FhirTerminologyValidation::mustRetry, _ -> Mono.error(InternalRetryableException::new))
                .toEntity(String.class)
                .retryWhen(
                        Retry.backoff(8, Duration.ofMillis(20)).filter(FhirTerminologyValidation::mustRetry));
    }

    private boolean isValidTerminology(String url) {
        boolean valid = url != null && Arrays.stream(ACCEPTED_FHIR_APIS).anyMatch(url::startsWith);
        if (!valid) {
            LOG.warn("Unsupported service-api: {}", url);
        }
        return valid;
    }

    @Override
    public boolean supports(TerminologyParam param) {
        return Optional.ofNullable(param)
                .map(TerminologyParam::serviceApi)
                .filter(this::isValidTerminology)
                .map(_ -> extractUrl(param.parameter()))
                .map(urlParam -> switch (param.resouceType()) {
                    case VALUE_SET -> SUPPORTS_VALUE_SET_TEMPL.formatted(baseUrl, urlParam);
                    case CODE_SYSTEM -> SUPPORTS_CODE_SYS_TEMPL.formatted(baseUrl, urlParam);
                })
                .map(url -> {
                    try {
                        return internalGet(url);
                    } catch (WebClientException e) {
                        if (failOnError) {
                            throw new ExternalTerminologyValidationException(ERR_SUPPORTS.formatted(url), e);
                        }
                        LOG.warn("The following error occurred: {}", e.getMessage());
                        return null;
                    }
                })
                .map(doc -> doc.read(SUPPORTS_TOTAL_JSON_PATH, int.class))
                .filter(t -> t > 0)
                .isPresent();
    }

    @Override
    public ConstraintViolation validate(TerminologyParam param) {
        String url = extractUrl(param.parameter());
        if (url == null) {
            return new ConstraintViolation("Missing value-set url");
        }

        return validateCode(url, param.codePhrase(), param.resouceType());
    }

    static String guaranteePrefix(String prefix, String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        } else if (str.contains(prefix)) {
            return str;
        } else {
            return prefix + str;
        }
    }

    private ConstraintViolation validateCode(
            String fhirTerminologyUri, CodePhrase codePhrase, TerminologyParam.ResouceType resouceType) {
        String code = codePhrase.getCodeString();

        String url =
                switch (resouceType) {
                    case VALUE_SET -> {
                        TerminologyId system = codePhrase.getTerminologyId();
                        yield VALUESET_VALIDATE_URL_TPL.formatted(baseUrl, fhirTerminologyUri, code, system);
                    }
                    case CODE_SYSTEM -> CODESYSTEM_VALIDATE_URL_TPL.formatted(baseUrl, fhirTerminologyUri, code);
                };

        DocumentContext context;
        try {
            context = internalGet(url);
        } catch (WebClientException e) {
            if (failOnError) {
                throw new ExternalTerminologyValidationException("An error occurred during $validate-code request", e);
            }
            LOG.warn("An error occurred while validating the code in CodeSystem: {}", e.getMessage());
            return null;
        }

        boolean noResult = context.<List<?>>read(VALIDATE_CODE_RESULT_JSON_PATH).isEmpty();
        if (noResult) {
            List<String> message = context.read(VALIDATE_CODE_MESSAGE_JSON_PATH);
            return new ConstraintViolation(String.join("; ", message));
        }

        return null;
    }


    private static boolean mustRetry(Throwable throwable) {
        return switch (throwable) {
            case InternalRetryableException _ -> true;
            case null, default -> false;
        };
    }

    static boolean mustRetry(HttpStatusCode statusCode) {
        return switch(statusCode) {
            case HttpStatus.BAD_GATEWAY,
                 HttpStatus.INTERNAL_SERVER_ERROR,
                 HttpStatus.GATEWAY_TIMEOUT,
                 HttpStatus.REQUEST_TIMEOUT,
                 HttpStatus.TOO_MANY_REQUESTS,
                 HttpStatus.NOT_FOUND -> true;
            default -> false;
        };
    }
}
