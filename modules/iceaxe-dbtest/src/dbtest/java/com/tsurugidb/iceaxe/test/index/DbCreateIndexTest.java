/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.test.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create index test
 */
class DbCreateIndexTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    private static final String CREATE_INDEX_SQL = "create index idx_test_bar on " + TEST + " (bar)";

    @Test
    void create_emptyTable() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.executeDdl(CREATE_INDEX_SQL);

        insertTestTable(4);
        assertIndex();
    }

    @Test
    void create_existsRecord() throws Exception {
        insertTestTable(4);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO レコードが存在していてもindexが作成できること
        var e = assertThrows(TsurugiTmIOException.class, () -> {
            tm.executeDdl(CREATE_INDEX_SQL);
        });
        assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);

//TODO        assertIndex();
    }

    private void assertIndex() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var sql = SELECT_SQL + " where bar=1";
            try (var ps = session.createQuery(sql, TgParameterMapping.of(), SELECT_MAPPING)) {
                var metadata = ps.explain(TgBindParameters.of());
                var planGraph = metadata.getLowPlanGraph();
                assertContains("index=idx_test_bar", planGraph.toString());

                var list = transaction.executeAndGetList(ps, TgBindParameters.of());
                assertEquals(1, list.size());
                assertEquals(createTestEntity(1), list.get(0));
            } catch (PlanGraphException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
