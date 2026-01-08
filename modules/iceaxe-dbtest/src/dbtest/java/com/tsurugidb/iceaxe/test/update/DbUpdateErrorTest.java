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
package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update error test
 */
class DbUpdateErrorTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        if (!info.getDisplayName().equals("updateNullToNotNull()")) {
            createTestTable();
            insertTestTable(SIZE);
        }

        logInitEnd(info);
    }

    @Test
    void updatePKNull() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = null" // primary key
                + " where foo = 5";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            var t = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.execute(transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps));
                    assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e);
                    assertContains("Null assigned for non-nullable field", e.getMessage()); // TODO エラー詳細情報の確認
                });
            });
            assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, t);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updatePKNullAll() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = null"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            var t = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.execute(transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps));
                    assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e);
                    assertContains("Null assigned for non-nullable field", e.getMessage()); // TODO エラー詳細情報の確認
                });
            });
            assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, t);
        }

        assertEqualsTestTable(SIZE);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, SIZE / 2, SIZE * 2 })
    void updatePKSameValue(int newPk) throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = " + newPk; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 2);
        try (var ps = session.createStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetCount(ps));
            assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);
            assertContains("Unique constraint violation occurred", e.getMessage()); // TODO エラー詳細情報（カラム名あるいは制約名）の確認
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateNullToNotNull() throws Exception {
        var session = getSession();

        var createSql = "create table " + TEST //
                + "(" //
                + "  foo int not null," //
                + "  bar bigint not null," //
                + "  zzz varchar(10) not null," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(session, createSql);
        insertTestTable(SIZE);

        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            var t = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.execute(transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps));
                    assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e);
                    assertContains("Null assigned for non-nullable field", e.getMessage()); // TODO エラー詳細情報の確認
                });
            });
            assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, t);
        }

        assertEqualsTestTable(SIZE);
    }
}
