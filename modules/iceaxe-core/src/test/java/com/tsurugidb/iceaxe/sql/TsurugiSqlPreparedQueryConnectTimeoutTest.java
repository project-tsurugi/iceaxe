/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

class TsurugiSqlPreparedQueryConnectTimeoutTest {

    @Test
    void connectTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.PS_CONNECT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testConnectTimeout(sessionOption, ps -> ps.setConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testConnectTimeout(TgSessionOption sessionOption, Consumer<TsurugiSqlPreparedQuery<?, ?>> modifier) throws Exception {
        var future = new TestFutureResponse<PreparedStatement>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestPreparedStatementFutureResponse(future);

            try (var ps = session.createQuery("SQL", TgParameterMapping.of())) {
                if (modifier != null) {
                    modifier.accept(ps);
                }

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> ps.getLowPreparedStatement());
                assertEquals(IceaxeErrorCode.PS_CONNECT_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(future.isClosed());
    }

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.PS_CONNECT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, ps -> ps.setConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSqlPreparedQuery<?, ?>> modifier) throws Exception {
        var future = new TestFutureResponse<PreparedStatement>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestPreparedStatementFutureResponse(future);

            try (var ps = session.createQuery("SQL", TgParameterMapping.of())) {
                if (modifier != null) {
                    modifier.accept(ps);
                }

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> ps.getLowPreparedStatement());
                assertEquals(IceaxeErrorCode.PS_CLOSE_TIMEOUT, e.getDiagnosticCode());

                future.setExpectedCloseTimeout(null);
                future.setThrowCloseTimeout(false);
            }
        }

        assertTrue(future.isClosed());
    }
}
