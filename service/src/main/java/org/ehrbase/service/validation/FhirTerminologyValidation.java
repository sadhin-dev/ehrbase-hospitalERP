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
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestBodyUriSpec;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/**
 * {@link ExternalTerminologyValidation} that supports FHIR terminology validation.
 */
public class FhirTerminologyValidation implements ExternalTerminologyValidation {

    static final String SUPPORTS_CODE_SYS_TEMPL = "%s/CodeSystem?_summary=true&url=%s";
    static final String SUPPORTS_VALUE_SET_TEMPL = "%s/ValueSet?_summary=true&url=%s";
    public static final JsonPath VALIDATE_CODE_RESULT_JSON_PATH =
            JsonPath.compile("$.parameter[?(@.name==\"result\" && \"valueBoolean\" == true)].valueBoolean");
    public static final JsonPath VALIDATE_CODE_MESSAGE_JSON_PATH =
            JsonPath.compile("$.parameter[?(@.name==\"message\")].valueString");
    public static final JsonPath SUPPORTS_TOTAL_JSON_PATH = JsonPath.compile("$.total");
    private static String VALUESET_VALIDATE_URL_TPL = "%s/ValueSet/$validate-code?url=%s&code=%s&system=%s";
    private static String CODESYSTEM_VALIDATE_URL_TPL = "%s/CodeSystem/$validate-code?url=%s&code=%s";

    private static final String ERR_SUPPORTS =
            "An error occurred while checking if FHIR terminology server supports the referenceSetUri: %s";

    private static final String ERR_EXPAND_VALUESET = "Error while expanding ValueSet[%s]";

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

    private String extractUrl(String referenceSetUri) {
        UriComponents uriComponents = UriComponentsBuilder.fromUriString("/foo?%s".formatted(referenceSetUri))
                .build();
        MultiValueMap<String, String> queryParams = uriComponents.getQueryParams();
        return queryParams.getFirst("url");
    }

    private WebClient buildRestClientCall(String url) {
        String uriValue = uriTagValue(url);
        // in the metrics call, uri in the uriTagValue function does not have the query parameters
        HttpClient client = HttpClient.create().metrics(true, _ -> uriValue).responseTimeout(Duration.ofSeconds(10));

        return webClientTemplate
                .mutate()
                .clientConnector(new ReactorClientHttpConnector(client))
                .baseUrl(url)
                .defaultHeader(HttpHeaders.ACCEPT, FHIR_ACCEPT_HEADER)
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
        String terminologyUrl = uc.getQueryParams().getFirst("url");
        return terminologyUrl == null ? path : path + "?url=" + terminologyUrl;
    }

    protected DocumentContext internalGet(String uri)
            throws WebClientException, ExternalTerminologyValidationException {

        WebClient client = buildRestClientCall(uri);
        RequestBodyUriSpec method = client.method(HttpMethod.GET);
        Mono<ResponseEntity<String>> mono = method.retrieve().toEntity(String.class);
        ResponseEntity<String> respEntity = Optional.ofNullable(mono.block())
                .orElseThrow(() -> new InternalServerException("could not connect to external Terminology Server"));

        String responseBody = respEntity.getBody();

        HttpStatusCode statusCode = respEntity.getStatusCode();
        if (statusCode.is2xxSuccessful()) {
            return JsonPath.parse(responseBody);
        } else {
            throw new ExternalTerminologyValidationException(
                    "Error response received from the terminology server. HTTP status: %s. Body: %s"
                            .formatted(statusCode, responseBody));
        }
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
                .flatMap(_ -> param.extractFromParameter(p -> Optional.ofNullable(extractUrl(p))))
                .map(urlParam -> {
                    if (param.useValueSet()) {
                        return SUPPORTS_VALUE_SET_TEMPL.formatted(baseUrl, urlParam);
                    } else {
                        return SUPPORTS_CODE_SYS_TEMPL.formatted(baseUrl, urlParam);
                    }
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
        String url = param.extractFromParameter(p -> Optional.ofNullable(extractUrl(p)))
                .orElse(null);

        if (url == null) {
            return new ConstraintViolation("Missing value-set url");
        }

        return validateCode(url, param.codePhrase(), param.useValueSet());
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

    private ConstraintViolation validateCode(String fhirTerminologyUri, CodePhrase codePhrase, boolean forValueSet) {
        String url;
        String code = codePhrase.getCodeString();
        if (forValueSet) {
            TerminologyId system = codePhrase.getTerminologyId();
            url = VALUESET_VALIDATE_URL_TPL.formatted(baseUrl, fhirTerminologyUri, code, system);
        } else {
            url = CODESYSTEM_VALIDATE_URL_TPL.formatted(baseUrl, fhirTerminologyUri, code);
        }
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
}
