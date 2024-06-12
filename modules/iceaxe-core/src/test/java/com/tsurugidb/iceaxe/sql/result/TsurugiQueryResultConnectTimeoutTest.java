package com.tsurugidb.iceaxe.sql.result;

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
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.ResultSet;

class TsurugiQueryResultConnectTimeoutTest {

    @Test
    void connectTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.RS_CONNECT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testConnectTimeout(sessionOption, rs -> rs.setRsConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testConnectTimeout(TgSessionOption sessionOption, Consumer<TsurugiQueryResult<?>> modifier) throws Exception {
        var future = new TestFutureResponse<ResultSet>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestResultSetFutureResponse(future);

                try (var ps = session.createQuery("SQL")) {
                    try (var rs = ps.execute(transaction)) {
                        if (modifier != null) {
                            modifier.accept(rs);
                        }

                        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> rs.getLowResultSet());
                        assertEquals(IceaxeErrorCode.RS_CONNECT_TIMEOUT, e.getDiagnosticCode());
                    }
                }
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
        sessionOption.setTimeout(TgTimeoutKey.RS_CONNECT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, rs -> rs.setRsConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiQueryResult<?>> modifier) throws Exception {
        var future = new TestFutureResponse<ResultSet>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestResultSetFutureResponse(future);

                try (var ps = session.createQuery("SQL")) {
                    try (var rs = ps.execute(transaction)) {
                        if (modifier != null) {
                            modifier.accept(rs);
                        }

                        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> rs.getLowResultSet());
                        assertEquals(IceaxeErrorCode.RS_CLOSE_TIMEOUT, e.getDiagnosticCode());

                        future.setExpectedCloseTimeout(null);
                        future.setThrowCloseTimeout(false);
                    }
                }
            }
        }

        assertTrue(future.isClosed());
    }
}
