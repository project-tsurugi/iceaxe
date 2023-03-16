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
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * delete test
 */
class DbDeleteTest extends DbTestTableTester {

    static final int SIZE = 4;

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
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
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
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(1, count);
        }

        assertEqualsDelete(number);
    }

    @Test
    void deleteByBind() throws IOException {
        int number = 2;

        var foo = TgBindVariable.ofInt("foo");
        var sql = "delete from " + TEST //
                + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(foo.bind(number));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
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
        try (var ps = session.createStatement(sql)) {
            int count1 = tm.executeAndGetCount(ps);
            assertUpdateCount(1, count1);

            int count2 = tm.executeAndGetCount(ps);
            assertUpdateCount(0, count2);
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
        try (var ps = session.createStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(ps);
                assertUpdateCount(1, count1);
                assertNothingInTx(session, transaction, number);

                int count2 = transaction.executeAndGetCount(ps);
                assertUpdateCount(0, count2);
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
        try (var ps1 = session.createStatement(sql1); //
                var ps2 = session.createStatement(sql2)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(ps1);
                assertUpdateCount(2, count1);
                assertNothingInTx(session, transaction, 1);
                assertNothingInTx(session, transaction, 2);

                int count2 = transaction.executeAndGetCount(ps2);
                assertUpdateCount(1, count2);
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
        try (var deletePs = session.createStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(deletePs);
                assertUpdateCount(1, count1);
                assertNothingInTx(session, transaction, number);

                var entity = createTestEntity(number);
                try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    int count2 = transaction.executeAndGetCount(insertPs, entity);
                    assertUpdateCount(1, count2);
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
        try (var deletePs = session.createStatement(sql)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(deletePs);
                assertUpdateCount(expected1, count1);
                assertNothingInTx(session, transaction, number);

                var entity = createTestEntity(number);
                try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    int count2 = transaction.executeAndGetCount(insertPs, entity);
                    assertUpdateCount(1, count2);
                }
                assertEqualsInTx(session, transaction, entity);

                int count3 = transaction.executeAndGetCount(deletePs);
                assertUpdateCount(1, count3);
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
        try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(insertPs, entity);
                assertUpdateCount(1, count1);
                assertEqualsInTx(session, transaction, entity);

                try (var deletePs = session.createStatement(sql)) {
                    int count2 = transaction.executeAndGetCount(deletePs);
                    assertUpdateCount(1, count2);
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
        try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                int count1 = transaction.executeAndGetCount(insertPs, entity1);
                assertUpdateCount(1, count1);
                assertEqualsInTx(session, transaction, entity1);

                try (var deletePs = session.createStatement(sql)) {
                    int count2 = transaction.executeAndGetCount(deletePs);
                    assertUpdateCount(1, count2);
                }
                assertNothingInTx(session, transaction, entity1.getFoo());

                int count3 = transaction.executeAndGetCount(insertPs, entity2);
                assertUpdateCount(1, count3);
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
        try (var ps = session.createQuery(sql)) {
            var actual = transaction.executeAndFindRecord(ps);
            assertTrue(actual.isEmpty());
        }
    }

    private void assertEqualsInTx(TsurugiSession session, TsurugiTransaction transaction, TestEntity expected) throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = " + expected.getFoo();
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var actual = transaction.executeAndFindRecord(ps).get();
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
