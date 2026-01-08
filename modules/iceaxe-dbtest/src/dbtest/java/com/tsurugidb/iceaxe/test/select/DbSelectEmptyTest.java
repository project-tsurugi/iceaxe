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
package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;

/**
 * select empty-table test
 */
class DbSelectEmptyTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = Link.responseBoxSize() + 100;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectEmptyTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void selectEmpty() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
                assertEquals(List.of(), list);
            }
        }
    }

    @Test
    void selectEmptySameTx() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            tm.execute(transaction -> {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    List<TsurugiResultEntity> list = transaction.executeAndGetList(ps);
                    assertEquals(List.of(), list);
                }
                return;
            });
        }
    }

    @Test
    void selectCount() throws Exception {
        selectCount("");
    }

    void selectCount(String where) throws IOException, InterruptedException {
        var sql = "select count(*) from " + TEST + where;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(0, count);
        }
    }

    @Test
    void selectSum() throws Exception {
        selectSum("");
    }

    void selectSum(String where) throws IOException, InterruptedException {
        var sql = "select sum(bar) as bar, min(zzz) as zzz from " + TEST + where;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            TsurugiResultEntity entity = tm.executeAndFindRecord(ps).get();
            assertNull(entity.getIntOrNull("bar"));
            assertNull(entity.getStringOrNull("zzz"));
        }
    }

    @Test
    void selectKeyCount() throws Exception {
        selectKeyCount("");
    }

    void selectKeyCount(String where) throws IOException, InterruptedException {
        var sql = "select foo, count(*) from " + TEST //
                + where //
                + " group by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(0, list.size());
        }
    }

    @Test
    void selectKeySum() throws Exception {
        selectKeySum("");
    }

    void selectKeySum(String where) throws IOException, InterruptedException {
        var sql = "select foo, sum(bar) as bar, min(zzz) as zzz from " + TEST //
                + where //
                + " group by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(0, list.size());
        }
    }
}
