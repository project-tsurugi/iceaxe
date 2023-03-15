package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TusurigQueryResult;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * {@link TusurigQueryResult} test
 */
class DbResultSetTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbResultSetTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void whileEach() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var rs = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), rs.getHasNextRow());

                    int[] count = { 0 };
                    rs.whileEach(entity -> {
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    });
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), rs.getHasNextRow());
                    assertEquals(SIZE, rs.getReadCount());
                }
            });
        }
    }

    @Test
    void getRecordList() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var rs = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), rs.getHasNextRow());

                    List<TestEntity> list = rs.getRecordList();
                    assertEqualsTestTable(SIZE, list);

                    assertEquals(Optional.of(false), rs.getHasNextRow());
                    assertEquals(SIZE, rs.getReadCount());
                }
            });
        }
    }

    @Test
    void findRecord() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL + " order by foo", SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var rs = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), rs.getHasNextRow());

                    Optional<TestEntity> recordOpt = rs.findRecord();
                    assertTrue(recordOpt.isPresent());
                    var expected = createTestEntity(0);
                    assertEquals(expected, recordOpt.get());

                    assertEquals(Optional.of(true), rs.getHasNextRow());
                    assertEquals(1, rs.getReadCount());
                }
            });
        }
    }

    @Test
    void iterator() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var rs = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), rs.getHasNextRow());

                    int[] count = { 0 };
                    for (Iterator<TestEntity> i = rs.iterator(); i.hasNext();) {
                        var entity = i.next();
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    }
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), rs.getHasNextRow());
                    assertEquals(SIZE, rs.getReadCount());
                }
            });
        }
    }

    @Test
    void forEach() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var rs = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), rs.getHasNextRow());

                    int[] count = { 0 };
                    rs.forEach(entity -> {
                        var expected = createTestEntity(count[0]++);
                        assertEquals(expected, entity);
                    });
                    assertEquals(SIZE, count[0]);

                    assertEquals(Optional.of(false), rs.getHasNextRow());
                    assertEquals(SIZE, rs.getReadCount());
                }
            });
        }
    }
}
