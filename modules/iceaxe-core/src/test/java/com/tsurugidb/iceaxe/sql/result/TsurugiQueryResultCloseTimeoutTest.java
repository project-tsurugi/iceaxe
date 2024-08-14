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
import com.tsurugidb.iceaxe.test.low.TestResultSet;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.ResultSet;

class TsurugiQueryResultCloseTimeoutTest {

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

//TODO    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.RS_CLOSE, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

//TODO    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, rs -> rs.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    // FIXME rs.close()内部でgetLowResultSet()を呼んでいるため、rs.close()でのfutureクローズタイムアウト単独は実行できない
    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiQueryResult<?>> modifier) throws Exception {
        var future = new TestFutureResponse<ResultSet>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestResultSetFutureResponse(future);

                try (var ps = session.createQuery("SQL")) {
                    var rs = ps.execute(transaction);
                    if (modifier != null) {
                        modifier.accept(rs);
                    }

                    var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> rs.close());
                    assertEquals(IceaxeErrorCode.RS_CLOSE_TIMEOUT, e.getDiagnosticCode());

                    future.setExpectedCloseTimeout(null);
                    future.setThrowCloseTimeout(false);
                }
            }
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
        sessionOption.setTimeout(TgTimeoutKey.RS_CLOSE, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testLowCloseTimeout(sessionOption, ps -> ps.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    @Test
    @SuppressWarnings("removal") // TODO remove this test
    void lowCloseTimeout_set_old() throws Exception {
        var sessionOption = TgSessionOption.of();

        testLowCloseTimeout(sessionOption, ps -> ps.setRsCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testLowCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiQueryResult<?>> modifier) throws Exception {
        var future = new TestFutureResponse<ResultSet>() {
            @Override
            protected ResultSet getInternal() {
                return new TestResultSet();
            }
        };

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestResultSetFutureResponse(future);

                try (var ps = session.createQuery("SQL")) {
                    var rs = ps.execute(transaction);
                    if (modifier != null) {
                        modifier.accept(rs);
                    }

                    var lowRs = (TestResultSet) rs.getLowResultSet();
                    lowRs.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
                    lowRs.setThrowCloseTimeout(true);

                    var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> rs.close());
                    assertEquals(IceaxeErrorCode.RS_CLOSE_TIMEOUT, e.getDiagnosticCode());

                    assertTrue(lowRs.isClosed());

                    lowRs.setExpectedCloseTimeout(null);
                    lowRs.setThrowCloseTimeout(false);
                }
            }
        }

        assertTrue(future.isClosed());
    }
}
