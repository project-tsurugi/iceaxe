package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * multiplex insert error test
 */
class DbErrorMultiplexInsertTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

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
    void insertMultiCheck1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            insertMulti(1, ExecuteType.RESULT_CHECK);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    void insertMultiCheckN(int size) throws IOException, TsurugiTransactionException {
        insertMulti(size, ExecuteType.RESULT_CHECK);
    }

    @Test
    void insertMultiClose1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            insertMulti(1, ExecuteType.CLOSE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    void insertMultiCloseN(int size) throws IOException, TsurugiTransactionException {
        insertMulti(size, ExecuteType.CLOSE_ONLY);
    }

    @Test
    void insertMulti1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            insertMulti(1, ExecuteType.EXEUTE_ONLY);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 16, 255, 256 })
    void insertMultiN(int size) throws IOException, TsurugiTransactionException {
        insertMulti(size, ExecuteType.EXEUTE_ONLY);
    }

    private enum ExecuteType {
        RESULT_CHECK, CLOSE_ONLY, EXEUTE_ONLY
    }

    private void insertMulti(int size, ExecuteType type) throws IOException, TsurugiTransactionException {
        int threshold = 255;
        try {
            insertMultiMain(size, type);
        } catch (TsurugiIOException e) {
            if (size < threshold) {
                throw e;
            }
            assertEqualsCode(SqlServiceCode.ERR_RESOURCE_LIMIT_REACHED, e);
            assertContains("creating transaction failed with error:err_resource_limit_reached", e.getMessage());
            LOG.warn("err_resource_limit_reached occur. size={}, type={}", size, type);
        }
    }

    private void insertMultiMain(int size, ExecuteType type) throws IOException, TsurugiTransactionException {
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
                    transaction.executeAndGetCount(ps, entity);
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
