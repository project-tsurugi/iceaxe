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
package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.FutureResponseCloseWrapper;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * {@link TsurugiSql} test
 */
class DbTsurugiSqlTest extends DbTestTableTester {

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
    void queryConstructorError() throws Exception {
        var session = DbTestConnector.createSession();

        session.close();
        try (var target = new TsurugiSqlQuery<>(session, SELECT_SQL, SELECT_MAPPING)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                target.initialize();
            });
            assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
        }
    }

    @Test
    void statementConstructorError() throws Exception {
        var session = DbTestConnector.createSession();

        session.close();
        try (var target = new TsurugiSqlStatement(session, INSERT_SQL)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                target.initialize();
            });
            assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
        }
    }

    @Test
    void preparedQueryConstructorError() throws Exception {
        var session = DbTestConnector.createSession();
        FutureResponseCloseWrapper<PreparedStatement> future;
        try (var client = SqlClient.attach(session.getLowSession())) {
            future = FutureResponseCloseWrapper.of(client.prepare(SELECT_SQL));
        }

        session.close();
        try (var target = new TsurugiSqlPreparedQuery<>(session, SELECT_SQL, null, SELECT_MAPPING)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                target.initialize(future);
            });
            assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
            assertTrue(future.isClosed());
        }
    }

    @Test
    void preparedStatementConstructorError() throws Exception {
        var session = DbTestConnector.createSession();
        FutureResponseCloseWrapper<PreparedStatement> future;
        try (var client = SqlClient.attach(session.getLowSession())) {
            future = FutureResponseCloseWrapper.of(client.prepare(INSERT_SQL, INSERT_MAPPING.toLowPlaceholderList()));
        }

        session.close();
        try (var target = new TsurugiSqlPreparedStatement<>(session, INSERT_SQL, INSERT_MAPPING)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                target.initialize(future);
            });
            assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
            assertTrue(future.isClosed());
        }
    }
}
