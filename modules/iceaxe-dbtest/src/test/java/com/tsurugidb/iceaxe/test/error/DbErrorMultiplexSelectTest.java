package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * multiplex select error test
 */
class DbErrorMultiplexSelectTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbErrorMultiplexSelectTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        logInitEnd(LOG, info);
    }

    @Test
    public void selectMultiRead1() throws Exception {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            selectMulti(1, ExecuteType.READ_ALL);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 240, 241, 255, 256, 300 })
    public void selectMultiReadN(int size) throws Exception {
        selectMulti(size, ExecuteType.READ_ALL);
    }

    @Test
    public void selectMultiClose1() throws Exception {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            selectMulti(1, ExecuteType.CLOSE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 240, 241, 255, 256, 300 })
    public void selectMultiCloseN(int size) throws Exception {
        selectMulti(size, ExecuteType.CLOSE_ONLY);
    }

    @Test
    void selectMulti1() throws Exception {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            selectMulti(1, ExecuteType.EXEUTE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 240, 241, 255, 256, 300 })
    void selectMultiN(int size) throws Exception {
        selectMulti(size, ExecuteType.EXEUTE_ONLY);
    }

    private enum ExecuteType {
        READ_ALL, CLOSE_ONLY, EXEUTE_ONLY
    }

    private void selectMulti(int size, ExecuteType type) throws IOException, InterruptedException, TsurugiTransactionException {
        try {
            selectMultiMain(size, type);
        } catch (TsurugiIOException e) {
            if (DbTestConnector.isTcp()) {
                if (size > 280) {
                    assertEqualsCode(SqlServiceCode.TRANSACTION_EXCEEDED_LIMIT_EXCEPTION, e);
                    LOG.info("(TCP)TRANSACTION_EXCEEDED_LIMIT_EXCEPTION occur. size={}, type={}", size, type);
                    return;
                }
            }
            if (DbTestConnector.isIpc()) {
                if (size > 280) {
                    assertEqualsCode(SqlServiceCode.TRANSACTION_EXCEEDED_LIMIT_EXCEPTION, e);
                    LOG.info("(IPC)TRANSACTION_EXCEEDED_LIMIT_EXCEPTION occur. size={}, type={}", size, type);
                    return;
                }
                if (size > 240) {
                    assertEqualsCode(SqlServiceCode.SQL_LIMIT_REACHED_EXCEPTION, e);
                    assertContains("creating output channel failed (maybe too many requests)", e.getMessage());
                    LOG.info("(IPC)SQL_LIMIT_REACHED_EXCEPTION occur. size={}, type={}", size, type);
                    return;
                }
            }
            throw e;
        }
    }

    private void selectMultiMain(int size, ExecuteType type) throws IOException, InterruptedException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < size; i++) {
                var transaction = session.createTransaction(TgTxOption.ofOCC());
                // transaction.close is called on session.close

                var ps = session.createQuery(sql);
                // ps.close is called on session.close

                switch (type) {
                default:
                case READ_ALL:
                    transaction.executeAndGetList(ps);
                    break;
                case CLOSE_ONLY:
                    try (var result = transaction.executeQuery(ps)) {
                    }
                    break;
                case EXEUTE_ONLY:
                    @SuppressWarnings("unused")
                    var result = transaction.executeQuery(ps);
                    // result.close is called on transaction.close
                    break;
                }

                // do not commit/rollback
            }
        }
    }
}
