package com.tsurugidb.iceaxe.test.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * delete test
 */
class DbDeleteTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void deleteAll() throws IOException {
        var sql = "delete from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        assertEqualsTestTable();
    }

    @Test
    void deleteConstant() throws IOException {
        int number = 2;
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO 1
        }

        assertEqualsDelete(number);
    }

    @Test
    void deleteByBind() throws IOException {
        int number = 2;

        var foo = TgVariable.ofInt4("foo");
        var sql = "delete from " + TEST //
                + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var plist = TgParameterList.of(foo.bind(number));
            int count = ps.executeAndGetCount(tm, plist);
            assertEquals(-1, count); // TODO 1
        }

        assertEqualsDelete(number);
    }

    @Test
    void delete2SeqTx() throws IOException {
        int number = 2;
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count1 = ps.executeAndGetCount(tm);
            assertEquals(-1, count1); // TODO 1

            int count2 = ps.executeAndGetCount(tm);
            assertEquals(-1, count2); // TODO 0
        }

        assertEqualsDelete(number);
    }

    @Test
    void delete2SameTx() throws IOException {
        int number = 2;
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = ps.executeAndGetCount(transaction);
                assertEquals(-1, count1); // TODO 1
                assertNothingInTx(session, transaction, number);

                int count2 = ps.executeAndGetCount(transaction);
                assertEquals(-1, count2); // TODO 0
                assertNothingInTx(session, transaction, number);
            });
        }

        assertEqualsDelete(number);
    }

    @Test
    void delete2Range() throws IOException {
        var sql1 = "delete from " + TEST + " where 1 <= foo and foo <= 2";
        var sql2 = "delete from " + TEST + " where 2 <= foo and foo <= 3";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps1 = session.createPreparedStatement(sql1); //
                var ps2 = session.createPreparedStatement(sql2)) {
            tm.execute(transaction -> {
                int count1 = ps1.executeAndGetCount(transaction);
                assertEquals(-1, count1); // TODO 2
                assertNothingInTx(session, transaction, 1);
                assertNothingInTx(session, transaction, 2);

                int count2 = ps2.executeAndGetCount(transaction);
                assertEquals(-1, count2); // TODO 1
                assertNothingInTx(session, transaction, 2);
                assertNothingInTx(session, transaction, 3);
            });
        }

        assertEqualsDelete(1, 2, 3);
    }

    @Test
    void deleteInsert() throws IOException {
        int number = 2;
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var deletePs = session.createPreparedStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = deletePs.executeAndGetCount(transaction);
                assertEquals(-1, count1); // TODO 1
                assertNothingInTx(session, transaction, number);

                var entity = createTestEntity(number);
                try (var insertPs = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                    int count2 = insertPs.executeAndGetCount(transaction, entity);
                    assertEquals(-1, count2); // TODO 1
                }
                assertEqualsInTx(session, transaction, entity);
            });
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void deleteInsertDeleteExists() throws IOException {
        int number = 2;
        assert number < SIZE;
        deleteInsertDelete(number, 1);
    }

    @Test
    void deleteInsertDeleteNotExists() throws IOException {
        int number = 123;
        assert number >= SIZE;
        deleteInsertDelete(number, 0);
    }

    private void deleteInsertDelete(int number, int expected1) throws IOException {
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var deletePs = session.createPreparedStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = deletePs.executeAndGetCount(transaction);
                assertEquals(-1, count1); // TODO expected1
                assertNothingInTx(session, transaction, number);

                var entity = createTestEntity(number);
                try (var insertPs = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                    int count2 = insertPs.executeAndGetCount(transaction, entity);
                    assertEquals(-1, count2); // TODO 1
                }
                assertEqualsInTx(session, transaction, entity);

                int count3 = deletePs.executeAndGetCount(transaction);
                assertEquals(-1, count3); // TODO 1
                assertNothingInTx(session, transaction, number);
            });
        }

        assertEqualsDelete(number);
    }

    @Test
    void insertDelete() throws IOException {
        var entity = new TestEntity(123, 456, "abc");
        var sql = "delete from " + TEST + " where foo = " + entity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var insertPs = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                int count1 = insertPs.executeAndGetCount(transaction, entity);
                assertEquals(-1, count1); // TODO 1
                assertEqualsInTx(session, transaction, entity);

                try (var deletePs = session.createPreparedStatement(sql)) {
                    int count2 = deletePs.executeAndGetCount(transaction);
                    assertEquals(-1, count2); // TODO 1
                }
                assertNothingInTx(session, transaction, entity.getFoo());
            });
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertDeleteInsert() throws IOException {
        var entity1 = new TestEntity(123, 456, "abc");
        var entity2 = new TestEntity(entity1.getFoo(), 999, "zzz");
        var sql = "delete from " + TEST + " where foo = " + entity1.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var insertPs = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                int count1 = insertPs.executeAndGetCount(transaction, entity1);
                assertEquals(-1, count1); // TODO 1
                assertEqualsInTx(session, transaction, entity1);

                try (var deletePs = session.createPreparedStatement(sql)) {
                    int count2 = deletePs.executeAndGetCount(transaction);
                    assertEquals(-1, count2); // TODO 1
                }
                assertNothingInTx(session, transaction, entity1.getFoo());

                int count3 = insertPs.executeAndGetCount(transaction, entity2);
                assertEquals(-1, count3); // TODO 1
                assertEqualsInTx(session, transaction, entity2);
            });
        }

        var expectedList = new ArrayList<TestEntity>(SIZE - 1);
        for (int i = 0; i < SIZE; i++) {
            var expected = createTestEntity(i);
            expectedList.add(expected);
        }
        expectedList.add(entity2);
        assertEqualsTestTable(expectedList);
    }

    private void assertNothingInTx(TsurugiSession session, TsurugiTransaction transaction, int foo) throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = " + foo;
        try (var ps = session.createPreparedQuery(sql)) {
            var actual = ps.executeAndFindRecord(transaction);
            assertTrue(actual.isEmpty());
        }
    }

    private void assertEqualsInTx(TsurugiSession session, TsurugiTransaction transaction, TestEntity expected) throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = " + expected.getFoo();
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            var actual = ps.executeAndFindRecord(transaction).get();
            assertEquals(expected, actual);
        }
    }

    private void assertEqualsDelete(int... numbers) throws IOException {
        var deleteSet = Arrays.stream(numbers).boxed().collect(Collectors.toSet());
        var expectedList = new ArrayList<TestEntity>(SIZE - 1);
        for (int i = 0; i < SIZE; i++) {
            if (deleteSet.contains(i)) {
                continue;
            }
            var expected = createTestEntity(i);
            expectedList.add(expected);
        }
        assertEqualsTestTable(expectedList);
    }
}