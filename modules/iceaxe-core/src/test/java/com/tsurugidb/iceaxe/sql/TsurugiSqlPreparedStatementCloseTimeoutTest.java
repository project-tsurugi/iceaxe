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
import com.tsurugidb.iceaxe.test.low.TestPreparedStatement;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

class TsurugiSqlPreparedStatementCloseTimeoutTest {

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.PS_CLOSE, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, ps -> ps.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSqlPreparedStatement<?>> modifier) throws Exception {
        var future = new TestFutureResponse<PreparedStatement>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestPreparedStatementFutureResponse(future);

            var ps = session.createStatement("SQL", TgParameterMapping.of());
            if (modifier != null) {
                modifier.accept(ps);
            }

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> ps.close());
            assertEquals(IceaxeErrorCode.PS_CLOSE_TIMEOUT, e.getDiagnosticCode());

            future.setExpectedCloseTimeout(null);
            future.setThrowCloseTimeout(false);
        }

        assertTrue(future.isClosed());
    }

    @Test
    void lowCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.PS_CLOSE, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testLowCloseTimeout(sessionOption, ps -> ps.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testLowCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSqlPreparedStatement<?>> modifier) throws Exception {
        var future = new TestFutureResponse<PreparedStatement>() {
            @Override
            protected PreparedStatement getInternal() {
                return new TestPreparedStatement(true);
            }
        };

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestPreparedStatementFutureResponse(future);

            var ps = session.createStatement("SQL", TgParameterMapping.of());
            if (modifier != null) {
                modifier.accept(ps);
            }

            var lowPs = (TestPreparedStatement) ps.getLowPreparedStatement();
            lowPs.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
            lowPs.setThrowCloseTimeout(true);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> ps.close());
            assertEquals(IceaxeErrorCode.PS_CLOSE_TIMEOUT, e.getDiagnosticCode());

            assertTrue(lowPs.isClosed());

            lowPs.setExpectedCloseTimeout(null);
            lowPs.setThrowCloseTimeout(false);
        }

        assertTrue(future.isClosed());
    }
}
