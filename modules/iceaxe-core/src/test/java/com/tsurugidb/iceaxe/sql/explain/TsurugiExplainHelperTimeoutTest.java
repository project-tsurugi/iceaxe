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
package com.tsurugidb.iceaxe.sql.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;

class TsurugiExplainHelperTimeoutTest {

    @Test
    void sqlConnectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.EXPLAIN_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<StatementMetadata>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestStatementMetadataFutureResponse(future);

            var target = new TsurugiExplainHelper();
            var connectTimeout = target.getConnectTimeout(sessionOption);
            @SuppressWarnings("deprecation")
            var closeTimeout = target.getCloseTimeout(sessionOption);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.explain(session, "SQL", connectTimeout, closeTimeout));
            assertEquals(IceaxeErrorCode.EXPLAIN_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void sqlCloseTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.EXPLAIN_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<StatementMetadata>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestStatementMetadataFutureResponse(future);

            var target = new TsurugiExplainHelper();
            var connectTimeout = target.getConnectTimeout(sessionOption);
            @SuppressWarnings("deprecation")
            var closeTimeout = target.getCloseTimeout(sessionOption);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.explain(session, "SQL", connectTimeout, closeTimeout));
            assertEquals(IceaxeErrorCode.EXPLAIN_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void psConnectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.EXPLAIN_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<StatementMetadata>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestStatementMetadataFutureResponse(future);

            var target = new TsurugiExplainHelper();
            var connectTimeout = target.getConnectTimeout(sessionOption);
            @SuppressWarnings("deprecation")
            var closeTimeout = target.getCloseTimeout(sessionOption);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.explain(session, "SQL", null, null, List.of(), connectTimeout, closeTimeout));
            assertEquals(IceaxeErrorCode.EXPLAIN_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void psCloseTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.EXPLAIN_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<StatementMetadata>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestStatementMetadataFutureResponse(future);

            var target = new TsurugiExplainHelper();
            var connectTimeout = target.getConnectTimeout(sessionOption);
            @SuppressWarnings("deprecation")
            var closeTimeout = target.getCloseTimeout(sessionOption);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.explain(session, "SQL", null, null, List.of(), connectTimeout, closeTimeout));
            assertEquals(IceaxeErrorCode.EXPLAIN_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }
}
