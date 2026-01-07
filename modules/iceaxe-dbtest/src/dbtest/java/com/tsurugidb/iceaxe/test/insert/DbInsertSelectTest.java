package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert select test
 */
class DbInsertSelectTest extends DbTestTableTester {

    private static final int SIZE = 4;
    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);
        createTest2Table();

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        dropTable(TEST2);
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql, TEST2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    private void insertSelect(String s) throws Exception {
        var txOption = toTxOption(s);

        var session = getSession();
        var sql = "insert into " + TEST2 + " select * from " + TEST;
        try (var ps = session.createStatement(sql)) {
            try (var transaction = session.createTransaction(txOption)) {
                var count = transaction.executeAndGetCountDetail(ps);
                assertEquals(1, count.getLowCounterMap().size());
                assertEquals(SIZE, count.getInsertedCount());

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        var tm = createTransactionManagerOcc(session);
        var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
        var expectedList = selectAllFromTest();
        assertEquals(expectedList, actualList);
    }

    private TgTxOption toTxOption(String s) {
        switch (s) {
        case "OCC":
            return TgTxOption.ofOCC();
        case "LTX":
            return TgTxOption.ofLTX(TEST2);
        default:
            throw new AssertionError(s);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void insertSelect2(String s) throws Exception {
        var txOption = toTxOption(s);

        var add = TgBindVariable.ofInt("add");
        var sql = "insert into " + TEST2 + " select foo+" + add + ", bar, zzz from " + TEST;
        var parameterMapping = TgParameterMapping.of(add);

        var session = getSession();
        try (var ps = session.createStatement(sql, parameterMapping)) {
            for (int i = 0, n = 0; i < 20; i++, n += SIZE) {
                try (var transaction = session.createTransaction(txOption)) {
                    var parameter = TgBindParameters.of(add.bind(n));
                    var count = transaction.executeAndGetCountDetail(ps, parameter);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE, count.getInsertedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }

                var tm = createTransactionManagerOcc(session);
                var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
                assertEquals(n + SIZE, actualList.size());
                int foo = 0;
                for (var actual : actualList) {
                    assertEquals(foo, actual.getFoo());
                    assertEquals(foo % SIZE, actual.getBar());
                    foo++;
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void insertSelect_duplicate(String s) throws Exception {
        var txOption = toTxOption(s);

        var session = getSession();
        {
            var sql = "insert into " + TEST2 + " select * from " + TEST;
            try (var ps = session.createStatement(sql)) {
                try (var transaction = session.createTransaction(txOption)) {
                    var count = transaction.executeAndGetCountDetail(ps);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE, count.getInsertedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }
            }

            var tm = createTransactionManagerOcc(session);
            var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
            var expectedList = selectAllFromTest();
            assertEquals(expectedList, actualList);
        }

        var sql = "insert into " + TEST2 + " select * from " + TEST;
        try (var ps = session.createStatement(sql)) {
            try (var transaction = session.createTransaction(txOption)) {
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCountDetail(ps);
                });
                assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void insertSelectSame(String s) throws Exception {
        var txOption = toTxOptionSame(s);

        var session = getSession();
        for (int i = 0, n = SIZE; i < 10; i++, n *= 2) {
            var sql = "insert into " + TEST + " select foo+" + n + ", bar, zzz from " + TEST;
            try (var ps = session.createStatement(sql)) {
                try (var transaction = session.createTransaction(txOption)) {
                    var count = transaction.executeAndGetCountDetail(ps);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(n, count.getInsertedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                } catch (TsurugiTransactionException e) {
                    if (txOption.isOCC()) {
                        assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                        assertContains("reason_code:CC_OCC_PHANTOM_AVOIDANCE", e.getMessage());
                        return;
                    }
                    throw e;
                }
            }
        }
    }

    private TgTxOption toTxOptionSame(String s) {
        switch (s) {
        case "OCC":
            return TgTxOption.ofOCC();
        case "LTX":
            return TgTxOption.ofLTX(TEST);
        default:
            throw new AssertionError(s);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void replaceSelect(String s) throws Exception {
        var txOption = toTxOption(s);

        var expectedList = new ArrayList<TestEntity>();

        var session = getSession();
        {
            var sql = "insert or replace into " + TEST2 + " select * from " + TEST;
            try (var ps = session.createStatement(sql)) {
                try (var transaction = session.createTransaction(txOption)) {
                    var count = transaction.executeAndGetCountDetail(ps);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE, count.getMergedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }
            }

            for (int i = 0; i < SIZE; i++) {
                var expected = createTestEntity(i);
                expectedList.add(expected);
            }

            var tm = createTransactionManagerOcc(session);
            var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
            assertEquals(expectedList, actualList);
        }

        var add = TgBindVariable.ofInt("add");
        var sql = "insert or replace into " + TEST2 + " select foo+" + add + ", bar, zzz from " + TEST;
        var parameterMapping = TgParameterMapping.of(add);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            for (int i = 0, n = SIZE - 1; i < 10; i++, n += SIZE - 1) {
                try (var transaction = session.createTransaction(txOption)) {
                    var parameter = TgBindParameters.of(add.bind(n));
                    var count = transaction.executeAndGetCountDetail(ps, parameter);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE, count.getMergedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }

                var remove = expectedList.remove(expectedList.size() - 1);
                assertEquals(n, remove.getFoo());
                for (int j = 0; j < SIZE; j++) {
                    var expected = createTestEntity(j);
                    expected.setFoo(expected.getFoo() + n);
                    expectedList.add(expected);
                }

                var tm = createTransactionManagerOcc(session);
                var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
                assertEquals(expectedList, actualList);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void ifNotExistsSelect(String s) throws Exception {
        ignoreSelect("insert if not exists", s);
    }

    @ParameterizedTest
    @ValueSource(strings = { "OCC", "LTX" })
    void ignoreSelect(String s) throws Exception {
        ignoreSelect("insert or ignore", s);
    }

    private void ignoreSelect(String insert, String s) throws Exception {
        var txOption = toTxOption(s);

        var expectedList = new ArrayList<TestEntity>();

        var session = getSession();
        {
            var sql = insert + " into " + TEST2 + " select * from " + TEST;
            try (var ps = session.createStatement(sql)) {
                try (var transaction = session.createTransaction(txOption)) {
                    var count = transaction.executeAndGetCountDetail(ps);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE, count.getInsertedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }
            }

            for (int i = 0; i < SIZE; i++) {
                var expected = createTestEntity(i);
                expectedList.add(expected);
            }

            var tm = createTransactionManagerOcc(session);
            var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
            assertEquals(expectedList, actualList);
        }

        var add = TgBindVariable.ofInt("add");
        var sql = insert + " into " + TEST2 + " select foo+" + add + ", bar, zzz from " + TEST;
        var parameterMapping = TgParameterMapping.of(add);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            for (int i = 0, n = SIZE - 1; i < 10; i++, n += SIZE - 1) {
                try (var transaction = session.createTransaction(txOption)) {
                    var parameter = TgBindParameters.of(add.bind(n));
                    var count = transaction.executeAndGetCountDetail(ps, parameter);
                    assertEquals(1, count.getLowCounterMap().size());
                    assertEquals(SIZE - 1, count.getInsertedCount());

                    transaction.commit(TgCommitType.DEFAULT);
                }

                for (int j = 1; j < SIZE; j++) {
                    var expected = createTestEntity(j);
                    expected.setFoo(expected.getFoo() + n);
                    expectedList.add(expected);
                }

                var tm = createTransactionManagerOcc(session);
                var actualList = tm.executeAndGetList("select * from " + TEST2 + " order by foo", SELECT_MAPPING);
                assertEquals(expectedList, actualList);
            }
        }
    }
}
