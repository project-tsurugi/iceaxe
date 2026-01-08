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
package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * session error test
 */
class DbSessionErrorTest extends DbTestTableTester {

    @Test
    void createQueryAfterClose() throws Exception {
        var session = createClosedSession();

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            session.createQuery(SELECT_SQL);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createPreparedQueryAfterClose() throws Exception {
        var session = createClosedSession();

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            session.createQuery(SELECT_SQL, TgParameterMapping.of());
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createStatementAfterClose() throws Exception {
        var session = createClosedSession();

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            session.createStatement(INSERT_SQL);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createPreparedStatementAfterClose() throws Exception {
        var session = createClosedSession();

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            session.createStatement(INSERT_SQL, INSERT_MAPPING);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createTransactionAfterClose() throws Exception {
        var session = createClosedSession();

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            session.createTransaction(TgTxOption.ofOCC());
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void craeteTmAfterClose() throws Exception {
        var session = createClosedSession();

        session.createTransactionManager(); // not thrown
    }

    private TsurugiSession createClosedSession() throws Exception {
        var session = DbTestConnector.createSession();
        try {
            assertTrue(session.isAlive());
        } catch (Throwable t) {
            session.close();
            throw t;
        }
        session.close();
        assertFalse(session.isAlive());

        return session;
    }
}
