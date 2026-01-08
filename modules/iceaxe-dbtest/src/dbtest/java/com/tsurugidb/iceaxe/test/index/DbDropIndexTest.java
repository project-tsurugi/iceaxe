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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;

/**
 * drop index test
 */
class DbDropIndexTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        executeDdl(getSession(), "create index idx_test_bar on " + TEST + " (bar)");

        logInitEnd(info);
    }

    @Test
    void drop() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        var sql = "drop index idx_test_bar";
        tm.executeDdl(sql);

        var e = assertThrows(TsurugiTmIOException.class, () -> {
            tm.executeDdl(sql);
        });
        assertErrorIndexNotFound("idx_test_bar", e);
    }

    @Test
    void dropTable() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        tm.executeDdl("drop table " + TEST);

        var e = assertThrows(TsurugiTmIOException.class, () -> {
            tm.executeDdl("drop index idx_test_bar");
        });
        assertErrorIndexNotFound("idx_test_bar", e);
    }
}
