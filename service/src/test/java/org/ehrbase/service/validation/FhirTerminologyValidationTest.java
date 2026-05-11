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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jayway.jsonpath.internal.JsonContext;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import net.java.quickcheck.generator.PrimitiveGenerators;
import org.ehrbase.openehr.sdk.validation.terminology.TerminologyParam;
import org.ehrbase.service.validation.ExternalTerminologyProviderProperties.ProviderType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class FhirTerminologyValidationTest {

    @Test
    void supports_ValidParams_ReturnsTrue() {
        String baseUrl = "http://terminology.local";
        String valueSetUrl = "http://snomed.info/sct?fhir_vs=ecl/%3C306206005";

        FhirTerminologyValidation validation = spy(new FhirTerminologyValidation(
                new ExternalTerminologyProviderProperties(ProviderType.FHIR, baseUrl, false)));
        TerminologyParam param = TerminologyParam.ofFhir(
                "//fhir.hl7.org/ValueSet/$expand?url=" + valueSetUrl + "&activeOnly=true", null);

        JsonContext jsonContext = mock(JsonContext.class);
        when(jsonContext.read(FhirTerminologyValidation.SUPPORTS_TOTAL_JSON_PATH, int.class))
                .thenReturn(1);

        doReturn(jsonContext).when(validation).internalGet(Mockito.anyString());

        assertTrue(validation.supports(param));

        verify(validation).internalGet(FhirTerminologyValidation.SUPPORTS_VALUE_SET_TEMPL.formatted(valueSetUrl));
    }

    @Test
    void supports_InvalidParams_ReturnsFalse() {
        FhirTerminologyValidation validation = spy(new FhirTerminologyValidation(
                new ExternalTerminologyProviderProperties(ProviderType.FHIR, "http://terminology.local", false)));

        assertFalse(
                validation.supports(TerminologyParam.ofFhir("//fhir.hl7.org/ValueSet/$expand?activeOnly=true", null)));
        assertFalse(validation.supports(TerminologyParam.ofFhir(
                "//invalid/ValueSet/$expand?url=http://snomed.info/sct?fhir_vs=ecl/%3C306206005&activeOnly=true",
                null)));

        verify(validation, never()).internalGet(Mockito.anyString());
    }

    @Test
    void extractUrl() {
        assertThat(FhirTerminologyValidation.extractUrl("code=12345")).isNull();
        Stream.of(
                        "url=http://snomed.info/sct",
                        "url=http://snomed.info/sct&code=12345",
                        "code=12345&url=http://snomed.info/sct")
                .forEach(
                        p -> assertThat(FhirTerminologyValidation.extractUrl(p)).isEqualTo("http://snomed.info/sct"));
    }

    static ValueSet anyValueSet() {
        List<ValueSetExpansionContainsComponent> values = IntStream.range(0, 16)
                .mapToObj(i -> anyValueSetExpansionContainsComponent())
                .toList();

        ValueSetExpansionComponent ext = new ValueSetExpansionComponent();
        ext.setId(anyString());
        ext.setContains(values);

        ValueSet valueSet = new ValueSet();
        valueSet.setId(anyString());
        valueSet.setExpansion(ext);

        return valueSet;
    }

    static ValueSetExpansionContainsComponent anyValueSetExpansionContainsComponent() {
        ValueSetExpansionContainsComponent cmp = new ValueSetExpansionContainsComponent();
        cmp.setId(anyString());
        cmp.setCode(anyString());
        cmp.setSystem(anyString());
        cmp.setDisplay(anyString());
        return cmp;
    }

    static String anyString() {
        return PrimitiveGenerators.letterStrings(1, 16).next();
    }
}
