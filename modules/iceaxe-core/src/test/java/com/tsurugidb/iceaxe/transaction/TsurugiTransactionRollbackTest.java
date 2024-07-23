package com.tsurugidb.iceaxe.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.sql.Transaction;

class TsurugiTransactionRollbackTest {

    @Test
    void rollback_rollbackSuccess() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            protected Transaction getInternal() {
                var lowTransaction = new TestLowTransaction();
                var rollbackFuture = new TestFutureResponse<Void>();
                lowTransaction.setTestRollbackFutureResponse(rollbackFuture);
                return lowTransaction;
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            boolean[] child1Closed = { false };
            var child1 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    child1Closed[0] = true;
                }
            };
            target.addChild(child1);

            target.rollback();

            assertTrue(target.isRollbacked());
            assertTrue(child1Closed[0]);
        }
    }

    @Test
    void rollback_rollbackException() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            protected Transaction getInternal() {
                var lowTransaction = new TestLowTransaction();
                var rollbackFuture = new TestFutureResponse<Void>() {
                    @Override
                    protected Void getInternal() throws IOException {
                        throw new IOException("rollback-error-test");
                    }
                };
                lowTransaction.setTestRollbackFutureResponse(rollbackFuture);
                return lowTransaction;
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            boolean[] child1Closed = { false };
            var child1 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    child1Closed[0] = true;
                }
            };
            target.addChild(child1);

            var e = assertThrowsExactly(IOException.class, () -> {
                target.rollback();
            });
            assertEquals("rollback-error-test", e.getMessage());

            assertFalse(target.isRollbacked());
            assertTrue(child1Closed[0]);
        }
    }

    @Test
    void rollback_childException_rollbackSuccess() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            protected Transaction getInternal() {
                var lowTransaction = new TestLowTransaction();
                var rollbackFuture = new TestFutureResponse<Void>();
                lowTransaction.setTestRollbackFutureResponse(rollbackFuture);
                return lowTransaction;
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            var child1 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    throw new IOException("test1");
                }
            };
            target.addChild(child1);

            var child2 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    throw new IOException("test2");
                }
            };
            target.addChild(child2);

            var e = assertThrowsExactly(IOException.class, () -> {
                target.rollback();
            });
            assertEquals("test1", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("test2", s0.getMessage());

            assertTrue(target.isRollbacked());
        }
    }

    @Test
    void rollback_childException_rollbackException() throws Exception {
        var session = new TsurugiSession(null, TgSessionOption.of());
        var future = new TestFutureResponse<Transaction>() {
            @Override
            protected Transaction getInternal() {
                var lowTransaction = new TestLowTransaction();
                var rollbackFuture = new TestFutureResponse<Void>() {
                    @Override
                    protected Void getInternal() throws IOException {
                        throw new IOException("rollback-error-test");
                    }
                };
                lowTransaction.setTestRollbackFutureResponse(rollbackFuture);
                return lowTransaction;
            }
        };

        try (var target = new TsurugiTransaction(session, TgTxOption.ofOCC())) {
            target.initialize(future);

            var child1 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    throw new IOException("test1");
                }
            };
            target.addChild(child1);

            var child2 = new IceaxeTimeoutCloseable() {
                @Override
                public void close(long timeoutNanos) throws Exception {
                    throw new IOException("test2");
                }
            };
            target.addChild(child2);

            var e = assertThrowsExactly(IOException.class, () -> {
                target.rollback();
            });
            assertEquals("rollback-error-test", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("test1", s0.getMessage());
            var s1 = e.getSuppressed()[1];
            assertInstanceOf(IOException.class, s1);
            assertEquals("test2", s1.getMessage());

            assertFalse(target.isRollbacked());
        }
    }
}
