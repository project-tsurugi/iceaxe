package com.tsurugidb.iceaxe.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Transaction;

class TsurugiTransactionTest {

    @Test
    void initialize_notCall() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            // do not call target.initialize(future);

            assertThrows(IllegalStateException.class, () -> {
                target.getLowTransaction();
            });
        }
    }

    @Test
    void initialize_twice1() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>();

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            assertThrows(IllegalStateException.class, () -> {
                target.initialize(future);
            });
        }
    }

    @Test
    void initialize_twice2() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            public Transaction get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException {
                return new Transaction() {
                    @Override
                    public String getTransactionId() {
                        return "TID-test";
                    }
                };
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            assertNotNull(target.getLowTransaction());
            assertThrows(IllegalStateException.class, () -> {
                target.initialize(future);
            });
        }
    }

    @Test
    void getLowTransactionError() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            public Transaction get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("test-exception");
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            var e1 = assertThrowsExactly(IOException.class, () -> {
                target.getLowTransaction();
            });
            assertEquals("test-exception", e1.getMessage());

            var e2 = assertThrowsExactly(IceaxeIOException.class, () -> {
                target.getLowTransaction();
            });
            assertEquals(IceaxeErrorCode.TX_LOW_ERROR, e2.getDiagnosticCode());
            assertSame(e1, e2.getCause());
        }
    }

    @Test
    void findEventListener() throws Exception {
        class TestListener implements TsurugiTransactionEventListener {
        }

        var session = new TsurugiSession(null, TgSessionOption.of());
        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            {
                var opt = target.findEventListener(l -> l instanceof TestListener);
                assertTrue(opt.isEmpty());
            }
            {
                var listener = new TestListener();
                target.addEventListener(listener);
                var opt = target.findEventListener(l -> l instanceof TestListener);
                assertSame(listener, opt.get());
            }
        }
    }
}
