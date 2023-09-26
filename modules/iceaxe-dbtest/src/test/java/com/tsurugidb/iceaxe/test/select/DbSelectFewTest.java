package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * select few record test
 */
class DbSelectFewTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 200;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectFewTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(4);

        logInitEnd(LOG, info);
    }

    @Test
    void selectZero() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                    try (var result = ps.execute(transaction)) {
                        // result.close without read
                    }
                    transaction.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }

    @Test
    void selectOne() throws Exception {
        var expected = createTestEntity(0);
        var sql = "select * from " + TEST + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                Optional<TestEntity> entity = tm.executeAndFindRecord(ps);
                assertEquals(expected, entity.get());
            }
        }
    }

    @Test
    void selectOneSameTx() throws Exception {
        var expected = createTestEntity(0);
        var sql = "select * from " + TEST + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    Optional<TestEntity> entity = transaction.executeAndFindRecord(ps);
                    assertEquals(expected, entity.get());
                }
                return;
            });
        }
    }
}
