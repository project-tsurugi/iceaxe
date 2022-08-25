package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * multiplex select error test
 */
class DbErrorMultiplexSelectTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbErrorMultiplexSelectTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        LOG.debug("init end");
    }

    @Test
    public void selectMultiRead1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            selectMulti(1, ExecuteType.READ_ALL);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    public void selectMultiReadN(int size) throws IOException, TsurugiTransactionException {
        selectMulti(size, ExecuteType.READ_ALL);
    }

    @Test
    @Disabled // TODO remove Disabled
    public void selectMultiClose1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            selectMulti(1, ExecuteType.CLOSE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    @Disabled // TODO remove Disabled
    public void selectMultiCloseN(int size) throws IOException, TsurugiTransactionException {
        selectMulti(size, ExecuteType.CLOSE_ONLY);
    }

    @Test
    @Disabled // TODO remove Disabled
    void selectMulti1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            selectMulti(1, ExecuteType.EXEUTE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15 })
    @Disabled // TODO remove Disabled
    void selectMultiN(int size) throws IOException, TsurugiTransactionException {
        selectMulti(size, ExecuteType.EXEUTE_ONLY);
    }

    @Test
    @Disabled // TODO change Disabled to Timeout
    void selectMulti16() throws IOException, TsurugiTransactionException {
        selectMulti(16, ExecuteType.EXEUTE_ONLY);
    }

    private enum ExecuteType {
        READ_ALL, CLOSE_ONLY, EXEUTE_ONLY
    }

    private void selectMulti(int size, ExecuteType type) throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < size; i++) {
                var transaction = session.createTransaction(TgTxOption.ofOCC());
                // transaction.close is called on session.close

                var ps = session.createPreparedQuery(sql);
                // ps.close is called on session.close

                switch (type) {
                default:
                case READ_ALL:
                    ps.executeAndGetList(transaction);
                    break;
                case CLOSE_ONLY:
                    try (var rs = ps.execute(transaction)) {
                    }
                    break;
                case EXEUTE_ONLY:
                    @SuppressWarnings("unused")
                    var rs = ps.execute(transaction);
                    // rs.close is called on ps.close
                    break;
                }

                // do not commit/rollback
            }
        }
    }
}
