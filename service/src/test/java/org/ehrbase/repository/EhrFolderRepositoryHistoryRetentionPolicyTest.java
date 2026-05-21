/*
 * Copyright (c) 2026 vitasystems GmbH.
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
package org.ehrbase.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehrbase.jooq.pg.Tables.EHR_FOLDER_DATA;
import static org.ehrbase.jooq.pg.Tables.EHR_FOLDER_VERSION;
import static org.ehrbase.jooq.pg.Tables.EHR_FOLDER_VERSION_HISTORY;
import static org.mockito.Mockito.mock;

import java.time.OffsetDateTime;
import java.util.List;
import org.ehrbase.api.service.SystemService;
import org.ehrbase.repository.AbstractVersionedObjectRepository.AdditionalCopyToHistoryFields;
import org.ehrbase.repository.AbstractVersionedObjectRepository.HistoryOperation;
import org.ehrbase.service.DirectoryProperties;
import org.ehrbase.service.DirectoryProperties.History;
import org.ehrbase.service.DirectoryProperties.History.RetentionPolicy;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EhrFolderRepositoryHistoryRetentionPolicyTest {

    private static final DSLContext CTX = DSL.using(SQLDialect.POSTGRES);

    private static final int OV_DATA_IDX = 3;
    private static final int OV_ITEM_UUIDS_IDX = 4;

    private static EhrFolderRepository repository(RetentionPolicy retentionPolicy) {
        return new EhrFolderRepository(
                CTX,
                mock(ContributionRepository.class),
                mock(SystemService.class),
                OffsetDateTime::now,
                new DirectoryProperties(new History(retentionPolicy)));
    }

    @ParameterizedTest
    @CsvSource({
        "ALL,       UPDATE, true",
        "ALL,       DELETE, true",
        "NONE,      UPDATE, false",
        "NONE,      DELETE, false",
        "ON_DELETE, UPDATE, false",
        "ON_DELETE, DELETE, true",
    })
    void historyDataRetention(RetentionPolicy retentionPolicy, HistoryOperation op, boolean expectedRetained) {

        AdditionalCopyToHistoryFields fields = repository(retentionPolicy)
                .additionalCopyToHistoryFields(EHR_FOLDER_VERSION, EHR_FOLDER_DATA, OffsetDateTime.now(), op);
        List<Field<?>> headFields = fields.headFields().toList();

        String ovDataSql = CTX.render(headFields.get(OV_DATA_IDX));
        String ovItemUuidsSql = CTX.render(headFields.get(OV_ITEM_UUIDS_IDX));

        String nullOvData = CTX.render(DSL.castNull(EHR_FOLDER_VERSION_HISTORY.OV_DATA.getDataType()));
        String nullOvItemUuids = CTX.render(DSL.castNull(EHR_FOLDER_VERSION_HISTORY.OV_ITEM_UUIDS.getDataType()));

        if (expectedRetained) {
            assertThat(ovDataSql).isNotEqualTo(nullOvData);
            assertThat(ovItemUuidsSql).isNotEqualTo(nullOvItemUuids);
        } else {
            assertThat(ovDataSql).isEqualTo(nullOvData);
            assertThat(ovItemUuidsSql).isEqualTo(nullOvItemUuids);
        }
    }
}
