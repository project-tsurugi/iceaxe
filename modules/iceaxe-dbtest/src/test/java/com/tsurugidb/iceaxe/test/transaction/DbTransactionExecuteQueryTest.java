package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction execute query test
 */
class DbTransactionExecuteQueryTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws IOException {
        var LOG = LoggerFactory.getLogger(DbTransactionExecuteQueryTest.class);
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void executeQuery() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var rs = transaction.executeQuery(ps)) {
                List<TestEntity> list = rs.getRecordList();
                assertEqualsTestTable(SIZE - 1, list);
            }
        }
    }

    @Test
    void executePreparedQuery() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(foo.bind(SIZE - 1));
            try (var rs = transaction.executeQuery(ps, parameter)) {
                List<TestEntity> list = rs.getRecordList();
                assertEqualsTestTable(SIZE - 1, list);
            }
        }
    }

    @Test
    void executeForEach() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int[] count = { 0 };
            transaction.executeForEach(ps, entity -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected, entity);
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executePreparedForEach() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(foo.bind(SIZE - 1));
            int[] count = { 0 };
            transaction.executeForEach(ps, parameter, entity -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected, entity);
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executeForEachRaw() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";
        var resultMapping = TgResultMapping.of(record -> record);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, resultMapping); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int[] count = { 0 };
            transaction.executeForEach(ps, record -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected.getFoo(), record.nextInt4OrNull());
                assertEquals(expected.getBar(), record.nextInt8OrNull());
                assertEquals(expected.getZzz(), record.nextCharacterOrNull());
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executePreparedForEachRaw() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var resultMapping = TgResultMapping.of(record -> record);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(foo.bind(SIZE - 1));
            int[] count = { 0 };
            transaction.executeForEach(ps, parameter, record -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected.getFoo(), record.nextInt4OrNull());
                assertEquals(expected.getBar(), record.nextInt8OrNull());
                assertEquals(expected.getZzz(), record.nextCharacterOrNull());
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executeAndFindRecord() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = transaction.executeAndFindRecord(ps).get();

            var expected = createTestEntity(1);
            assertEquals(expected, entity);
        }
    }

    @Test
    void executePreparedAndFindRecord() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(foo.bind(1));
            var entity = transaction.executeAndFindRecord(ps, parameter).get();

            var expected = createTestEntity(1);
            assertEquals(expected, entity);
        }
    }

    @Test
    void executeAndGetList() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var list = transaction.executeAndGetList(ps);

            assertEqualsTestTable(SIZE - 1, list);
        }
    }

    @Test
    void executePreparedAndGetList() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(foo.bind(SIZE - 1));
            var list = transaction.executeAndGetList(ps, parameter);

            assertEqualsTestTable(SIZE - 1, list);
        }
    }
}
