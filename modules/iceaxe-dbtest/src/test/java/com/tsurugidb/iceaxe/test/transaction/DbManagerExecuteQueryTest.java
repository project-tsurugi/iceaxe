package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TransactionManager execute query test
 */
class DbManagerExecuteQueryTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws IOException {
        var LOG = LoggerFactory.getLogger(DbManagerExecuteQueryTest.class);
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void executeAndFindRecord_sql() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(sql).get();

        var expected = createTestEntity(1);
        assertEqualsResultEntity(expected, entity);
    }

    @Test
    void executeAndFindRecord_setting_sql() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(setting, sql).get();

        var expected = createTestEntity(1);
        assertEqualsResultEntity(expected, entity);
    }

    @Test
    void executeAndFindRecord_sql_result() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(sql, SELECT_MAPPING).get();

        var expected = createTestEntity(1);
        assertEquals(expected, entity);
    }

    @Test
    void executeAndFindRecord_setting_sql_result() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(setting, sql, SELECT_MAPPING).get();

        var expected = createTestEntity(1);
        assertEquals(expected, entity);
    }

    @Test
    void executeAndFindRecord_sql_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(sql, parameterMapping, parameter).get();

        var expected = createTestEntity(1);
        assertEqualsResultEntity(expected, entity);
    }

    @Test
    void executeAndFindRecord_setting_sql_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(setting, sql, parameterMapping, parameter).get();

        var expected = createTestEntity(1);
        assertEqualsResultEntity(expected, entity);
    }

    @Test
    void executeAndFindRecord_sql_parameter_result() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(sql, parameterMapping, parameter, SELECT_MAPPING).get();

        var expected = createTestEntity(1);
        assertEquals(expected, entity);
    }

    @Test
    void executeAndFindRecord_setting_sql_parameter_result() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var entity = tm.executeAndFindRecord(setting, sql, parameterMapping, parameter, SELECT_MAPPING).get();

        var expected = createTestEntity(1);
        assertEquals(expected, entity);
    }

    @Test
    void executeAndFindRecord_ps() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql)) {
            var entity = tm.executeAndFindRecord(ps).get();

            var expected = createTestEntity(1);
            assertEqualsResultEntity(expected, entity);
        }
    }

    @Test
    void executeAndFindRecord_setting_ps() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo = 1";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql)) {
            var entity = tm.executeAndFindRecord(setting, ps).get();

            var expected = createTestEntity(1);
            assertEqualsResultEntity(expected, entity);
        }
    }

    @Test
    void executeAndFindRecord_ps_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
            var entity = tm.executeAndFindRecord(ps, parameter).get();

            var expected = createTestEntity(1);
            assertEqualsResultEntity(expected, entity);
        }
    }

    @Test
    void executeAndFindRecord_setting_ps_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
            var entity = tm.executeAndFindRecord(setting, ps, parameter).get();

            var expected = createTestEntity(1);
            assertEqualsResultEntity(expected, entity);
        }
    }

    @Test
    void executeAndGetList_sql() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(sql);

        assertEqualsTestTableResultEntity(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_setting_sql() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(setting, sql);

        assertEqualsTestTableResultEntity(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_sql_result() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(sql, SELECT_MAPPING);

        assertEqualsTestTable(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_setting_sql_result() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(setting, sql, SELECT_MAPPING);

        assertEqualsTestTable(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_sql_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(sql, parameterMapping, parameter);

        assertEqualsTestTableResultEntity(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_setting_sql_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(setting, sql, parameterMapping, parameter);

        assertEqualsTestTableResultEntity(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_sql_parameter_result() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(sql, parameterMapping, parameter, SELECT_MAPPING);

        assertEqualsTestTable(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_setting_sql_parameter_result() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var list = tm.executeAndGetList(setting, sql, parameterMapping, parameter, SELECT_MAPPING);

        assertEqualsTestTable(SIZE - 1, list);
    }

    @Test
    void executeAndGetList_ps() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql)) {
            var list = tm.executeAndGetList(ps);

            assertEqualsTestTableResultEntity(SIZE - 1, list);
        }
    }

    @Test
    void executeAndGetList_setting_ps() throws IOException, TsurugiTransactionException {
        var sql = SELECT_SQL + " where foo < " + (SIZE - 1) + " order by foo";

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql)) {
            var list = tm.executeAndGetList(setting, ps);

            assertEqualsTestTableResultEntity(SIZE - 1, list);
        }
    }

    @Test
    void executeAndGetList_ps_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
            var list = tm.executeAndGetList(ps, parameter);

            assertEqualsTestTableResultEntity(SIZE - 1, list);
        }
    }

    @Test
    void executeAndGetList_setting_ps_parameter() throws IOException, TsurugiTransactionException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo < " + foo + " order by foo";
        var parameterMapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(SIZE - 1));

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
            var list = tm.executeAndGetList(setting, ps, parameter);

            assertEqualsTestTableResultEntity(SIZE - 1, list);
        }
    }

    private static void assertEqualsTestTableResultEntity(int expectedSize, List<TsurugiResultEntity> actualList) {
        assertEquals(expectedSize, actualList.size());
        for (int i = 0; i < expectedSize; i++) {
            var expected = createTestEntity(i);
            var actual = actualList.get(i);
            assertEqualsResultEntity(expected, actual);
        }
    }
}
