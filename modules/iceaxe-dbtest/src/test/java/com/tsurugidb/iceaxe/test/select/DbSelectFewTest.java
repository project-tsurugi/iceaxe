package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * select few record test
 */
class DbSelectFewTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 200;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectFewTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(4);

        LOG.debug("init end");
    }

    @Test
    void selectZero() throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                    try (var rs = ps.execute(transaction)) {
                        // rs.close without read
                    }
                    transaction.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }

    @Test
    void selectOne() throws IOException {
        var expected = createTestEntity(0);
        var sql = "select * from " + TEST + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                Optional<TestEntity> entity = ps.executeAndFindRecord(tm);
                assertEquals(expected, entity.get());
            }
        }
    }

    @Test
    void selectOneSameTx() throws IOException {
        var expected = createTestEntity(0);
        var sql = "select * from " + TEST + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    Optional<TestEntity> entity = ps.executeAndFindRecord(transaction);
                    assertEquals(expected, entity.get());
                }
            });
        }
    }
}
