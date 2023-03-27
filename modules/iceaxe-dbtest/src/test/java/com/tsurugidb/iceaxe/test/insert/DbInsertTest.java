package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * insert test
 */
class DbInsertTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertConstant(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(" + entity.getFoo() + ", " + entity.getBar() + ", '" + entity.getZzz() + "')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(1, count);
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByBindVariables(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(:foo, :bar, :zzz)";
        var variables = TgBindVariables.of() //
                .addInt("foo") //
                .addLong("bar") //
                .addString("zzz");
        var parameterMapping = TgParameterMapping.of(variables);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of() //
                    .addInt("foo", entity.getFoo()) //
                    .addLong("bar", entity.getBar()) //
                    .addString("zzz", entity.getZzz());
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByBind(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var zzz = TgBindVariable.ofString("zzz");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of( //
                    foo.bind(entity.getFoo()), //
                    bar.bind(entity.getBar()), //
                    zzz.bind(entity.getZzz()));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByEntity(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(:foo, :bar, :zzz)";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            int count = tm.executeAndGetCount(ps, entity);
            assertUpdateCount(1, count);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertMany() throws IOException {
        var entityList = List.of( //
                new TestEntity(123, 456, "abc"), //
                new TestEntity(234, 789, "def"));

        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(:foo, :bar, :zzz)";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (var entity : entityList) {
                    int count = transaction.executeAndGetCount(ps, entity);
                    assertUpdateCount(1, count);
                }
            });
        }

        assertEqualsTestTable(entityList);
    }

    @Test
    void insertResultCheck() throws IOException {
        insertResultCheck(true);
    }

    @Test
    void insertResultNoCheck() throws IOException {
        insertResultCheck(false);
    }

    private void insertResultCheck(boolean resultCheck) throws IOException {
        int size = 100;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);

                    if (resultCheck) {
                        try (var result = ps.execute(transaction, entity)) {
                            var count = result.getUpdateCount();
                            assertUpdateCount(1, count);
                        }
                    } else {
                        @SuppressWarnings("unused")
                        var result = ps.execute(transaction, entity);
                        // result.close is called on transaction.close
                    }
                }
                return;
            });
        }

        assertEqualsTestTable(size);
    }

    @Test
    void insertPart() throws IOException {
        int key = 1;
        var foo = TgBindVariable.ofInt("foo");
        var sql = "insert into " + TEST + "(foo) values(" + foo + ")";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(foo.bind(key));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }

        var actual = selectFromTest(key);
        assertEquals(key, actual.getFoo());
        assertNull(actual.getBar());
        assertNull(actual.getZzz());
    }
}
