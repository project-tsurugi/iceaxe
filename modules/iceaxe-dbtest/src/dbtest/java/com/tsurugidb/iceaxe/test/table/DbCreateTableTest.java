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
package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table test
 */
class DbCreateTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    private static final String SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";

    @Test
    void create() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.executeDdl(SQL);
    }

    @Test
    void createExists() throws Exception {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeDdl(SQL);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("compile failed with error:table_already_exists message:\"table is already defined: " + TEST + "\" location:<input>:", e.getMessage());
    }

    @Test
    void alreadyExists() throws Exception {
        var session = getSession();
        var txOption = TgTxOption.ofDDL();

        // preparedStatementを作る際にテーブルが存在していないので、ERR_COMPILER_ERRORにならない
        try (var ps = session.createStatement(CREATE_TEST_SQL, TgParameterMapping.of())) {
            Thread.sleep(100); // preparedStatement作成がDBサーバー側で処理されるのを待つ
            // テーブルを作成する
            createTestTable();

            // テーブルが作られた後にpreparedStatementのDDLを実行するとERR_ALREADY_EXISTSになる
            try (var transaction = session.createTransaction(txOption)) {
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps, TgBindParameters.of()));
                assertEqualsCode(SqlServiceCode.TARGET_ALREADY_EXISTS_EXCEPTION, e);
                transaction.rollback();
            }
        }

        try (var ps = session.createStatement(CREATE_TEST_SQL, TgParameterMapping.of())) {
            try (var transaction = session.createTransaction(txOption)) {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.executeAndGetCount(ps, TgBindParameters.of()));
                assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
                assertContains("compile failed with error:table_already_exists message:\"table is already defined: " + TEST + "\" location:<input>:", e.getMessage());
                transaction.rollback();
            }
        }
    }

    @Test
    void rollback() throws Exception {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL)) {
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps);
                assertTrue(session.findTableMetadata(TEST).isPresent());
                transaction.rollback();
                assertTrue(session.findTableMetadata(TEST).isPresent());
            });
        }

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void repeatOcc(boolean hasPk) throws Exception {
        repeat(TgTxOption.ofOCC(), hasPk, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void repeatLtx(boolean hasPk) throws Exception {
        Consumer<TsurugiTmIOException> error = null;
        if (!hasPk) {
            error = e -> {
                assertEqualsCode(SqlServiceCode.LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION, e);
                assertContains("Ltx write operation outside write preserve", e.getMessage()); // TODO エラー詳細情報（テーブル名）
            };
        }
        repeat(TgTxOption.ofLTX().includeDdl(false), hasPk, error);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void repeatDdl(boolean hasPk) throws Exception {
        repeat(TgTxOption.ofDDL(), hasPk, null);
    }

    private void repeat(TgTxOption txOption, boolean hasPk, Consumer<TsurugiTmIOException> error) throws IOException, InterruptedException {
        var createDdl = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + (hasPk ? ", primary key(foo)" : "") //
                + ")";
        var dropDdl = "drop table " + TEST;

        var session = getSession();
        var tm = session.createTransactionManager(txOption);
        for (int i = 0; i < 100; i++) {
            if (session.findTableMetadata(TEST).isPresent()) {
                tm.executeDdl(dropDdl);
            }

            if (error == null) {
                tm.executeDdl(createDdl);
                assertTrue(session.findTableMetadata(TEST).isPresent());
            } else {
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                    tm.executeDdl(createDdl);
                });
                error.accept(e);
                assertTrue(session.findTableMetadata(TEST).isEmpty());
            }
        }
    }

    @Test
    void createLtxNoPk() throws Exception {
        var createDdl = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofLTX().includeDdl(false));
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.execute(transaction -> {
                var e1 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeDdl(createDdl);
                });
                assertEqualsCode(SqlServiceCode.LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION, e1);
                assertContains("Ltx write operation outside write preserve", e1.getMessage()); // TODO エラー詳細情報（テーブル名）

                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeDdl(createDdl);
                });
                assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, e2);
                assertContains("Current transaction is inactive (maybe aborted already.)", e2.getMessage());
            });
        });
        assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, e);
    }

    @Test
    void createIfNotExists() throws Exception {
        var sql = SQL.replace("create table", "create table if not exists");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        assertFalse(existsTable(TEST));
        tm.executeDdl(sql);
        assertTrue(existsTable(TEST));
        tm.executeDdl(sql);
        assertTrue(existsTable(TEST));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 0, -1 })
    void defaultValue(int defaultValue) throws Exception {
        var sql = "create table " + TEST + "(" //
                + " foo int primary key," //
                + " bar bigint default " + defaultValue + "," //
                + " zzz varchar(10) default 'd" + defaultValue + "'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(sql);

        tm.executeAndGetCount("insert into " + TEST + " values(1,11,'a')");
        tm.executeAndGetCount("insert into " + TEST + "(foo) values(2)");
        tm.executeAndGetCount("insert into " + TEST + "(foo, bar) values(3, 33)");
        tm.executeAndGetCount("insert into " + TEST + "(foo, zzz) values(4, '444')");

        var list = selectAllFromTest();
        assertEquals(4, list.size());
        var entity0 = list.get(0);
        assertEquals(1, entity0.getFoo());
        assertEquals(11L, entity0.getBar());
        assertEquals("a", entity0.getZzz());
        var entity1 = list.get(1);
        assertEquals(2, entity1.getFoo());
        assertEquals(defaultValue, entity1.getBar());
        assertEquals("d" + defaultValue, entity1.getZzz());
        var entity2 = list.get(2);
        assertEquals(3, entity2.getFoo());
        assertEquals(33L, entity2.getBar());
        assertEquals("d" + defaultValue, entity2.getZzz());
        var entity3 = list.get(3);
        assertEquals(4, entity3.getFoo());
        assertEquals(defaultValue, entity3.getBar());
        assertEquals("444", entity3.getZzz());
    }
}
