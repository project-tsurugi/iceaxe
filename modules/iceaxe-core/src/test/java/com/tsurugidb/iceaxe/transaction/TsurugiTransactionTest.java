package com.tsurugidb.iceaxe.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class TsurugiTransactionTest {

    @Test
    void getLowTransactionError() throws IOException {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new FutureResponse<Transaction>() {
            @Override
            public boolean isDone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Transaction get() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Transaction get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("test");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                // do nothing
            }
        };

        try (var target = new TsurugiTransaction(session, future, TgTxOption.ofOCC())) {
            var e1 = assertThrowsExactly(IOException.class, () -> target.getLowTransaction());
            assertEquals("test", e1.getMessage());
            var e2 = assertThrowsExactly(TsurugiIOException.class, () -> target.getLowTransaction());
            assertEquals(IceaxeErrorCode.TX_LOW_ERROR, e2.getDiagnosticCode());
            assertSame(e1, e2.getCause());
        }
    }
}
