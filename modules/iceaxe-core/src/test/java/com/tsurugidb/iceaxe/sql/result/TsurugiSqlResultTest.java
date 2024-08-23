package com.tsurugidb.iceaxe.sql.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

class TsurugiSqlResultTest {

    private static boolean backupDefaultEnableCheckResultOnClose;

    @BeforeAll
    static void beforeAll() {
        backupDefaultEnableCheckResultOnClose = TsurugiSqlResult.getDefaultEnableCheckResultOnClose();
    }

    @AfterEach
    void afterEach() {
        TsurugiSqlResult.setDefaultEnableCheckResultOnClose(backupDefaultEnableCheckResultOnClose);
    }

    static class TestSqlResult extends TsurugiSqlResult {

        public static TestSqlResult create() {
            var sessionOption = TgSessionOption.of();
            var session = new TestTsurugiSession(sessionOption);
            var transaction = new TsurugiTransaction(session, TgTxOption.ofOCC());
            return new TestSqlResult(transaction);
        }

        private TsurugiSession session;

        public TestSqlResult(TsurugiTransaction transaction) {
            super(0, transaction, null, null, null, null);
            this.session = transaction.getSession();
        }

        @Override
        public void close(long timeoutNanos) throws Exception {
            try (var c = session) {
                // close only
            }
        }
    }

    @Test
    void enableCheckResultOnClose() throws Exception {
        try (var result = TestSqlResult.create()) {
            assertEquals(backupDefaultEnableCheckResultOnClose, result.enableCheckResultOnClose());

            TsurugiSqlResult.setDefaultEnableCheckResultOnClose(false);
            assertFalse(result.enableCheckResultOnClose());
            TsurugiSqlResult.setDefaultEnableCheckResultOnClose(true);
            assertTrue(result.enableCheckResultOnClose());

            TsurugiSqlResult.setDefaultEnableCheckResultOnClose(false);
            result.setEnableCheckResultOnClose(true);
            assertTrue(result.enableCheckResultOnClose());

            TsurugiSqlResult.setDefaultEnableCheckResultOnClose(true);
            result.setEnableCheckResultOnClose(false);
            assertFalse(result.enableCheckResultOnClose());
        }
    }
}
