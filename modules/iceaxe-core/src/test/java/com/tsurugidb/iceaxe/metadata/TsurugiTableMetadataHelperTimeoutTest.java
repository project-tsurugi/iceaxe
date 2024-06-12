package com.tsurugidb.iceaxe.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;

class TsurugiTableMetadataHelperTimeoutTest {

    @Test
    void connectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_METADATA_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableMetadata>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableMetadataFutureResponse(future);

            var target = new TsurugiTableMetadataHelper();

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.findTableMetadata(session, "test"));
            assertEquals(IceaxeErrorCode.TABLE_METADATA_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void closeTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_METADATA_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableMetadata>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableMetadataFutureResponse(future);

            var target = new TsurugiTableMetadataHelper();

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.findTableMetadata(session, "test"));
            assertEquals(IceaxeErrorCode.TABLE_METADATA_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }
}
