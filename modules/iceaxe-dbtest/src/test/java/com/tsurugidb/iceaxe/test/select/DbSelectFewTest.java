package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager.TsurugiTransactionConsumer;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * select few record test
 */
class DbSelectFewTest extends DbTestTableTester {
    private static final Logger LOG = LoggerFactory.getLogger(DbSelectFewTest.class);

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
    @Disabled // TODO remove Disabled
    void selectZero() throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;
        LOG.info(sql);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql)) {
            for (int i = 0; i < 100; i++) {
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
            for (int i = 0; i < 300; i++) {
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
            tm.execute((TsurugiTransactionConsumer) transaction -> {
                for (int i = 0; i < 300; i++) {
                    Optional<TestEntity> entity = ps.executeAndFindRecord(transaction);
                    assertEquals(expected, entity.get());
                }
            });
        }
    }
}
