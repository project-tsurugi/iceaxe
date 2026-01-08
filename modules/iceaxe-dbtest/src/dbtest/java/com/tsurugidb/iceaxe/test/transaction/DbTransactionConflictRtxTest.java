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
package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction conflict test
 */
class DbTransactionConflictRtxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    private static final TgTxOption OCC = TgTxOption.ofOCC();
    private static final TgTxOption LTX = TgTxOption.ofLTX(TEST);
    private static final TgTxOption RTX = TgTxOption.ofRTX();

    private static final int KEY = 1;
    private static final long BAR_BEFORE = 1;
    private static final long BAR_AFTER2 = 999;
    private static final String SELECT_SQL1 = SELECT_SQL + " where foo = " + KEY;
    private static final String UPDATE_SQL2 = "update " + TEST + " set bar =  " + BAR_AFTER2 + " where foo = " + KEY;
    private static final String DELETE_SQL = "delete from " + TEST + " where foo = " + (SIZE - 1);

    @Test
    void rtx_occR() throws Exception {
        rtx_r(OCC);
    }

    @Test
    void rtx_rtx() throws Exception {
        rtx_r(RTX);
    }

    private void rtx_r(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(txOption2)) {
                    var entity2 = tx2.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity2.getBar());

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void rtx_occW() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(OCC)) {
                    tx2.executeAndGetCount(updatePs2);

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void rtx_ltx(boolean commitAsc) throws Exception {
        rtx_ltx(RTX, commitAsc);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void rtx_ltx_testByLTX(boolean commitAsc) throws Exception {
        rtx_ltx(TgTxOption.ofLTX(), commitAsc);
    }

    private void rtx_ltx(TgTxOption tx1Option, boolean commitAsc) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(tx1Option)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    if (commitAsc) {
                        var entity12 = tx1.executeAndFindRecord(selectPs).get();
                        assertEquals(BAR_BEFORE, entity12.getBar());

                        tx1.commit(TgCommitType.DEFAULT);
                        tx2.commit(TgCommitType.DEFAULT);
                    } else {
                        tx2.commit(TgCommitType.DEFAULT);

                        var entity12 = tx1.executeAndFindRecord(selectPs).get();
                        assertEquals(BAR_BEFORE, entity12.getBar());

                        tx1.commit(TgCommitType.DEFAULT);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void rtx_occW_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(OCC)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var list12 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list12.size());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + add);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void rtx_ltx_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(LTX)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    tx2.commit(TgCommitType.DEFAULT);

                    var list12 = tx1.executeAndGetList(selectPs);
                    assertEquals(SIZE, list12.size());

                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }

        assertEqualsTestTable(SIZE + add);
    }
}
