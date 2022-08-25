package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * multiplex insert error test
 */
class DbErrorMultiplexInsertTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach() throws IOException {
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        var entity = new TestEntity(1024, 456, "abc");
        insertTestTable(entity);

        LOG.debug("init end");
    }

    @Test
    void isnertMultiCheck1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            isnertMulti(1, ExecuteType.RESULT_CHECK);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    void isnertMultiCheckN(int size) throws IOException, TsurugiTransactionException {
        isnertMulti(size, ExecuteType.RESULT_CHECK);
    }

    @Test
    @Disabled // TODO remove Disabled
    void isnertMultiClose1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            isnertMulti(1, ExecuteType.CLOSE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    @Disabled // TODO remove Disabled
    void isnertMultiCloseN(int size) throws IOException, TsurugiTransactionException {
        isnertMulti(size, ExecuteType.CLOSE_ONLY);
    }

    @Test
    @Disabled // TODO remove Disabled
    void isnertMulti1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            isnertMulti(1, ExecuteType.EXEUTE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    @Disabled // TODO remove Disabled
    void isnertMultiN(int size) throws IOException, TsurugiTransactionException {
        isnertMulti(size, ExecuteType.EXEUTE_ONLY);
    }

    private enum ExecuteType {
        RESULT_CHECK, CLOSE_ONLY, EXEUTE_ONLY
    }

    private void isnertMulti(int size, ExecuteType type) throws IOException, TsurugiTransactionException {
        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < size; i++) {
                var transaction = session.createTransaction(TgTxOption.ofOCC());
                // transaction.close is called on session.close

                var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING);
                // ps.close is called on session.close

                var entity = createTestEntity(i);
                switch (type) {
                case RESULT_CHECK:
                default:
                    ps.executeAndGetCount(transaction, entity);
                    break;
                case CLOSE_ONLY:
                    try (var rs = ps.execute(transaction, entity)) {
                    }
                    break;
                case EXEUTE_ONLY:
                    @SuppressWarnings("unused")
                    var rs = ps.execute(transaction, entity);
                    // rs.close is called on ps.close
                    break;
                }

                // do not commit/rollback
            }
        }
    }
}
