package com.tsurugidb.iceaxe.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.TableList;

class TsurugiTableListHelperTimeoutTest {

    @Test
    void connectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_LIST_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableList>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableListFutureResponse(future);

            var target = new TsurugiTableListHelper();

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.getTableList(session));
            assertEquals(IceaxeErrorCode.TABLE_LIST_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void closeTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_LIST_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableList>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableListFutureResponse(future);

            var target = new TsurugiTableListHelper();

            var e = assertThrowsExactly(IceaxeIOException.class, () -> target.getTableList(session));
            assertEquals(IceaxeErrorCode.TABLE_LIST_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }
}
