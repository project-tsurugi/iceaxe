package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction execute query test
 */
class DbTransactionExecuteQueryTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTransactionExecuteQueryTest.class);
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void executeQuery() throws Exception {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var result = transaction.executeQuery(ps)) {
                List<TestEntity> list = result.getRecordList();
                assertEqualsTestTable(SIZE - 1, list);
            }
        }
    }

    @Test
    void executePreparedQuery() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(foo.bind(SIZE - 1));
            try (var result = transaction.executeQuery(ps, parameter)) {
                List<TestEntity> list = result.getRecordList();
                assertEqualsTestTable(SIZE - 1, list);
            }
        }
    }

    @Test
    void executeAndForEach() throws Exception {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int[] count = { 0 };
            transaction.executeAndForEach(ps, entity -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected, entity);
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executePreparedAndForEach() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(foo.bind(SIZE - 1));
            int[] count = { 0 };
            transaction.executeAndForEach(ps, parameter, entity -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected, entity);
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executeAndForEachRaw() throws Exception {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";
        var resultMapping = TgResultMapping.of(record -> record);

        var session = getSession();
        try (var ps = session.createQuery(sql, resultMapping); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int[] count = { 0 };
            transaction.executeAndForEach(ps, record -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected.getFoo(), record.nextIntOrNull());
                assertEquals(expected.getBar(), record.nextLongOrNull());
                assertEquals(expected.getZzz(), record.nextStringOrNull());
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executePreparedAndForEachRaw() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var resultMapping = TgResultMapping.of(record -> record);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(foo.bind(SIZE - 1));
            int[] count = { 0 };
            transaction.executeAndForEach(ps, parameter, record -> {
                var expected = createTestEntity(count[0]++);
                assertEquals(expected.getFoo(), record.nextIntOrNull());
                assertEquals(expected.getBar(), record.nextLongOrNull());
                assertEquals(expected.getZzz(), record.nextStringOrNull());
            });
            assertEquals(SIZE - 1, count[0]);
        }
    }

    @Test
    void executeAndFindRecord() throws Exception {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        try (var ps = session.createQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = transaction.executeAndFindRecord(ps).get();

            var expected = createTestEntity(1);
            assertEquals(expected, entity);
        }
    }

    @Test
    void executePreparedAndFindRecord() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(foo.bind(1));
            var entity = transaction.executeAndFindRecord(ps, parameter).get();

            var expected = createTestEntity(1);
            assertEquals(expected, entity);
        }
    }

    @Test
    void executeAndGetList() throws Exception {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        try (var ps = session.createQuery(sql, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var list = transaction.executeAndGetList(ps);

            assertEqualsTestTable(SIZE - 1, list);
        }
    }

    @Test
    void executePreparedAndGetList() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(foo.bind(SIZE - 1));
            var list = transaction.executeAndGetList(ps, parameter);

            assertEqualsTestTable(SIZE - 1, list);
        }
    }
}
