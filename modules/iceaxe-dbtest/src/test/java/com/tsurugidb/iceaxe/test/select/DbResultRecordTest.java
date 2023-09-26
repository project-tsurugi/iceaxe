package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * {@link TsurugiResultRecord} test
 */
class DbResultRecordTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbResultRecordTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void getInt_null() throws Exception {
        var sql = "select max(foo) as foo from " + TEST + " where foo < 0";
        var resultMapping = TgResultMapping.of(record -> record.getInt("foo"));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            var e = assertThrowsExactly(NullPointerException.class, () -> {
                tm.execute(transaction -> {
                    transaction.executeAndFindRecord(ps);
                });
            });
            assertEquals("TsurugiResultRecord.getInt(foo) is null", e.getMessage());
        }
    }

    @Test
    void nextInt_null() throws Exception {
        var sql = "select max(foo) from " + TEST + " where foo < 0";
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            var e = assertThrowsExactly(NullPointerException.class, () -> {
                tm.execute(transaction -> {
                    transaction.executeAndFindRecord(ps);
                });
            });
            assertEquals("TsurugiResultRecord.nextInt(0) is null", e.getMessage());
        }
    }
}
