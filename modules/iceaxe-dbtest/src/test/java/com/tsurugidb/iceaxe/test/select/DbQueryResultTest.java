package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * {@link TsurugiQueryResult} test
 */
class DbQueryResultTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws Exception {
        var LOG = LoggerFactory.getLogger(DbQueryResultTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void whileEach() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    int[] count = { 0 };
                    result.whileEach(entity -> {
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    });
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), result.getHasNextRow());
                    assertEquals(SIZE, result.getReadCount());
                }
            });
        }
    }

    @Test
    void getRecordList() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    List<TestEntity> list = result.getRecordList();
                    assertEqualsTestTable(SIZE, list);

                    assertEquals(Optional.of(false), result.getHasNextRow());
                    assertEquals(SIZE, result.getReadCount());
                }
            });
        }
    }

    @Test
    void findRecord() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL + " order by foo", SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    Optional<TestEntity> recordOpt = result.findRecord();
                    assertTrue(recordOpt.isPresent());
                    var expected = createTestEntity(0);
                    assertEquals(expected, recordOpt.get());

                    assertEquals(Optional.of(true), result.getHasNextRow());
                    assertEquals(1, result.getReadCount());
                }
            });
        }
    }

    @Test
    void iterator() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    int[] count = { 0 };
                    for (Iterator<TestEntity> i = result.iterator(); i.hasNext();) {
                        var entity = i.next();
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    }
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), result.getHasNextRow());
                    assertEquals(SIZE, result.getReadCount());
                }
            });
        }
    }

    @Test
    void forEach() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    int[] count = { 0 };
                    result.forEach(entity -> {
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    });
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), result.getHasNextRow());
                    assertEquals(SIZE, result.getReadCount());
                }
            });
        }
    }
}
