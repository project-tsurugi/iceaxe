package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TgResultCount;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.CounterType;

/**
 * {@link TgResultCount} test
 */
class DbResultCountTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void insert() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        tm.execute(transaction -> {
            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE);
                var result = transaction.executeAndGetCountDetail(ps, entity);
                assertCount(CounterType.INSERTED_ROWS, 1, result);
            }
        });
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void update() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        tm.execute(transaction -> {
            var sql = "update " + TEST + " set bar = 123 where foo < 0";
            try (var ps = session.createStatement(sql)) {
                var result = transaction.executeAndGetCountDetail(ps);
                assertCount(CounterType.UPDATED_ROWS, 0, result);
            }
        });
        assertEqualsTestTable(SIZE);

        long updateValue = 999;
        tm.execute(transaction -> {
            var sql = "update " + TEST + " set bar = " + updateValue;
            try (var ps = session.createStatement(sql)) {
                var result = transaction.executeAndGetCountDetail(ps);
                assertCount(CounterType.UPDATED_ROWS, SIZE, result);
            }
        });
        assertEqualsTestTable(SIZE, entity -> entity.setBar(updateValue));
    }

    @Test
    void merge() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        tm.execute(transaction -> {
            var sql = INSERT_SQL.replace("insert into", "insert or replace into");
            try (var ps = session.createStatement(sql, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE / 2);
                var result = transaction.executeAndGetCountDetail(ps, entity);
                assertCount(CounterType.MERGED_ROWS, 1, result);
            }
        });
        assertEqualsTestTable(SIZE);
    }

    @Test
    void delete() throws Exception {
        int deleteSize = 3;

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        tm.execute(transaction -> {
            var sql = "delete from " + TEST + " where foo >= " + (SIZE - deleteSize);
            try (var ps = session.createStatement(sql)) {
                var result = transaction.executeAndGetCountDetail(ps);
                assertCount(CounterType.DELETED_ROWS, deleteSize, result);
            }
        });
        assertEqualsTestTable(SIZE - deleteSize);

        tm.execute(transaction -> {
            var sql = "delete from " + TEST;
            try (var ps = session.createStatement(sql)) {
                var result = transaction.executeAndGetCountDetail(ps);
                assertCount(CounterType.DELETED_ROWS, SIZE - deleteSize, result);
            }
        });
        assertEqualsTestTable(0);

        tm.execute(transaction -> {
            var sql = "delete from " + TEST;
            try (var ps = session.createStatement(sql)) {
                var result = transaction.executeAndGetCountDetail(ps);
                assertCount(CounterType.DELETED_ROWS, 0, result);
            }
        });
        assertEqualsTestTable(0);
    }

    private static void assertCount(CounterType expectedType, long expectedCount, TgResultCount result) {
        assertEquals(Map.of(expectedType, expectedCount), result.getLowCounterMap());
        assertEquals(expectedCount, result.getTotalCount());
        for (var type : CounterType.values()) {
            long expected = (type == expectedType) ? expectedCount : 0;
            assertEquals(expected, result.getCount(type));
            assertEquals(expected, getCount(result, type));
        }
    }

    private static long getCount(TgResultCount result, CounterType type) {
        switch (type) {
        case INSERTED_ROWS:
            return result.getInsertedCount();
        case UPDATED_ROWS:
            return result.getUpdatedCount();
        case MERGED_ROWS:
            return result.getMergedCount();
        case DELETED_ROWS:
            return result.getDeletedCount();
        default:
            return 0;
        }
    }
}
