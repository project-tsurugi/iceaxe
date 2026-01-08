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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction test
 */
class DbTransactionWriteCrownLtxTest extends DbTestTableTester {

    private static final String TB1 = "tb1";
    private static final int SIZE = 6;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTable(TB1);
        createTbTable(TB1);

        logInitEnd(info);
    }

    private static void createTbTable(String tableName) throws IOException, InterruptedException {
        var sql = "create table " + tableName + "(" //
                + "id int primary key," //
                + "value int" //
                + ")";
        executeDdl(getSession(), sql, tableName);
    }

    private static void insert(String tableName, int id, int value) throws IOException, InterruptedException {
        var sql = "insert into " + tableName + " values(" + id + ", " + value + ")";
        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndGetCount(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "AVAILABLE", "STORED" })
    void test1(String commitTypeName) throws Exception {
        insert(TB1, 1, 100);

        var session = getSession();
        try (var select1Ps = session.createQuery("select * from " + TB1); //
                var insert1Ps = session.createStatement("insert into " + TB1 + " values(2, 200)"); //
                var select2Ps = session.createQuery("select * from " + TB1); //
                var insert2Ps = session.createStatement("insert into " + TB1 + " values(3, 300)")) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                var list1 = tx1.executeAndGetList(select1Ps);
                assertEquals(1, list1.size());
                var entity1 = list1.get(0);
                assertEquals(1, entity1.getInt("id"));
                assertEquals(100, entity1.getInt("value"));
                int count1 = tx1.executeAndGetCount(insert1Ps);
                assertUpdateCount(1, count1);

                try (var tx2 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                    var list2 = tx2.executeAndGetList(select2Ps);
                    var entity2 = list2.get(0);
                    assertEquals(1, entity2.getInt("id"));
                    assertEquals(100, entity2.getInt("value"));
                    int count2 = tx2.executeAndGetCount(insert2Ps);
                    assertUpdateCount(1, count2);

                    var commitType = toCommitType(commitTypeName);
                    tx1.commit(commitType);
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(commitType);
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "AVAILABLE", "STORED" })
    void test2_update(String commitTypeName) throws Exception {
        var session = getSession();
        try (var select1Ps = session.createQuery("select * from " + TB1 + " where id=1"); //
                var insert1Ps = session.createStatement("insert into " + TB1 + " values(6, 600)"); //
                var select2Ps = session.createQuery("select * from " + TB1); //
                var update2Ps = session.createStatement("update " + TB1 + " set value = 100 where id=1")) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                var list1 = tx1.executeAndGetList(select1Ps);
                assertEquals(0, list1.size());
                int count1 = tx1.executeAndGetCount(insert1Ps);
                assertUpdateCount(1, count1);

                try (var tx2 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                    var list2 = tx2.executeAndGetList(select2Ps);
                    assertEquals(0, list2.size());
                    int count2 = tx2.executeAndGetCount(update2Ps);
                    assertUpdateCount(0, count2);

                    var commitType = toCommitType(commitTypeName);
                    tx1.commit(commitType);
                    tx2.commit(commitType);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "AVAILABLE", "STORED" })
    void test2_insert(String commitTypeName) throws Exception {
        var session = getSession();
        try (var select1Ps = session.createQuery("select * from " + TB1 + " where id=1"); //
                var insert1Ps = session.createStatement("insert into " + TB1 + " values(6, 600)"); //
                var select2Ps = session.createQuery("select * from " + TB1); //
                var insert2Ps = session.createStatement("insert into " + TB1 + " values(1, 100)")) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                var list1 = tx1.executeAndGetList(select1Ps);
                assertEquals(0, list1.size());
                int count1 = tx1.executeAndGetCount(insert1Ps);
                assertUpdateCount(1, count1);

                try (var tx2 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                    var list2 = tx2.executeAndGetList(select2Ps);
                    assertEquals(0, list2.size());
                    int count2 = tx2.executeAndGetCount(insert2Ps);
                    assertUpdateCount(1, count2);

                    var commitType = toCommitType(commitTypeName);
                    tx1.commit(commitType);
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(commitType);
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "AVAILABLE", "STORED" })
    void test3(String commitTypeName) throws Exception {
        for (int i = 1; i <= SIZE; i++) {
            insert(TB1, i, i * 100);
        }

        var session = getSession();
        try (var select1Ps = session.createQuery("select * from " + TB1 + " where id=1"); //
                var insert1Ps = session.createStatement("insert into " + TB1 + " values(7, 700)"); //
                var select2Ps = session.createQuery("select * from " + TB1 + " where id>=2"); //
                var update2Ps = session.createStatement("update " + TB1 + " set value = 150 where id=1")) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                var list1 = tx1.executeAndGetList(select1Ps);
                assertEquals(1, list1.size());
                var entity1 = list1.get(0);
                assertEquals(1, entity1.getInt("id"));
                assertEquals(100, entity1.getInt("value"));
                int count1 = tx1.executeAndGetCount(insert1Ps);
                assertUpdateCount(1, count1);

                try (var tx2 = session.createTransaction(TgTxOption.ofLTX(TB1))) {
                    var list2 = tx2.executeAndGetList(select2Ps);
                    assertEquals(SIZE - 1, list2.size());
                    for (var entity2 : list2) {
                        int id2 = entity2.getInt("id");
                        if (id2 < 2 || SIZE < id2) {
                            fail("id2=" + id2);
                        }
                        assertEquals(id2 * 100, entity2.getInt("value"));
                    }
                    int count2 = tx2.executeAndGetCount(update2Ps);
                    assertUpdateCount(1, count2);

                    var commitType = toCommitType(commitTypeName);
                    tx1.commit(commitType);
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(commitType);
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                }
            }
        }
    }

    private static TgCommitType toCommitType(String commitType) {
        return TgCommitType.valueOf(commitType);
    }
}
