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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionException} test
 */
class DbTransactionExceptionTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void insertConstant() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 456, 'abc')";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        try (var ps = session.createStatement(sql)) {
            var e0 = assertThrows(TsurugiTmIOException.class, () -> tm.execute((TsurugiTransactionAction) transaction -> {
                var e = assertThrows(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps));
                assertEquals(transaction.getIceaxeTxId(), e.getIceaxeTxId());
                assertEquals(transaction.getIceaxeTmExecuteId(), e.getIceaxeTmExecuteId());
                assertEquals(transaction.getAttempt(), e.getAttempt());
                assertSame(transaction.getTransactionOption(), e.getTransactionOption());
                assertEquals(transaction.getTransactionId(), e.getTransactionId());
                assertEquals(TgTxMethod.EXECUTE_GET_COUNT, e.getTxMethod());
                assertSame(ps, e.getSqlDefinition());
                assertNull(e.getSqlParameter());

                throw e;
            }));
            assertEquals(TgTxMethod.EXECUTE_GET_COUNT, e0.getTxMethod());
            assertSame(ps, e0.getSqlDefinition());
            assertNull(e0.getSqlParameter());
        }
    }

    @Test
    void insertParameter() throws Exception {
        var entity = new TestEntity(1, 456, "abc");

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrows(TsurugiTmIOException.class, () -> tm.execute((TsurugiTransactionAction) transaction -> {
                var e = assertThrows(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps, entity));
                assertEquals(transaction.getIceaxeTxId(), e.getIceaxeTxId());
                assertEquals(transaction.getIceaxeTmExecuteId(), e.getIceaxeTmExecuteId());
                assertEquals(transaction.getAttempt(), e.getAttempt());
                assertSame(transaction.getTransactionOption(), e.getTransactionOption());
                assertEquals(transaction.getTransactionId(), e.getTransactionId());
                assertEquals(TgTxMethod.EXECUTE_GET_COUNT, e.getTxMethod());
                assertSame(ps, e.getSqlDefinition());
                assertSame(entity, e.getSqlParameter());

                throw e;
            }));
            assertEquals(TgTxMethod.EXECUTE_GET_COUNT, e0.getTxMethod());
            assertSame(ps, e0.getSqlDefinition());
            assertSame(entity, e0.getSqlParameter());
        }
    }

    @Test
    void insertResult() throws Exception {
        var entity = new TestEntity(1, 456, "abc");

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            TsurugiStatementResult[] result = { null };
            var e0 = assertThrows(TsurugiTmIOException.class, () -> tm.execute((TsurugiTransactionAction) transaction -> {
                var e = assertThrows(TsurugiTransactionException.class, () -> {
                    result[0] = transaction.executeStatement(ps, entity);
                    result[0].getUpdateCount();
                });
                assertEquals(transaction.getIceaxeTxId(), e.getIceaxeTxId());
                assertEquals(transaction.getIceaxeTmExecuteId(), e.getIceaxeTmExecuteId());
                assertEquals(transaction.getAttempt(), e.getAttempt());
                assertSame(transaction.getTransactionOption(), e.getTransactionOption());
                assertEquals(transaction.getTransactionId(), e.getTransactionId());
                assertNull(e.getTxMethod());
                assertSame(ps, e.getSqlDefinition());
                assertSame(entity, e.getSqlParameter());
                assertEquals(result[0].getIceaxeSqlExecuteId(), e.getIceaxeSqlExecuteId());

                throw e;
            }));
            assertNull(e0.getTxMethod());
            assertSame(ps, e0.getSqlDefinition());
            assertSame(entity, e0.getSqlParameter());
            assertEquals(result[0].getIceaxeSqlExecuteId(), e0.getIceaxeSqlExecuteId());
        }
    }
}
