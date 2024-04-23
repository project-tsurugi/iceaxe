package com.tsurugidb.iceaxe.sql.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
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

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.explain(session, "SQL", connectTimeout, closeTimeout));
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

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.explain(session, "SQL", connectTimeout, closeTimeout));
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

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.explain(session, "SQL", null, null, List.of(), connectTimeout, closeTimeout));
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

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.explain(session, "SQL", null, null, List.of(), connectTimeout, closeTimeout));
            assertEquals(IceaxeErrorCode.EXPLAIN_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }
}
