package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

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
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertConstant(boolean columns) throws Exception {
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
    void insertByBindVariables(boolean columns) throws Exception {
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
    void insertByBind(boolean columns) throws Exception {
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
    void insertByEntityMapping(boolean columns) throws Exception {
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

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByEntityConverter(boolean columns) throws Exception {
        var variables = TgBindVariables.of().addInt("foo").addLong("bar").addString("zzz");
        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(" + variables.getSqlNames() + ")";
        Function<TestEntity, TgBindParameters> parameterConverter = entity -> TgBindParameters.of().add("foo", entity.getFoo()).add("bar", entity.getBar()).add("zzz", entity.getZzz());
        var parameterMapping = TgParameterMapping.of(variables, parameterConverter);

        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            int count = tm.executeAndGetCount(ps, entity);
            assertUpdateCount(1, count);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertMany() throws Exception {
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
    void insertResultCheck() throws Exception {
        insertResultCheck(true);
    }

    @Test
    void insertResultNoCheck() throws Exception {
        insertResultCheck(false);
    }

    private void insertResultCheck(boolean resultCheck) throws IOException, InterruptedException {
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
    void insertPart() throws Exception {
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

    @Test
    void insertMultipleValues() throws Exception {
        var sql = new StringBuilder("insert into " + TEST + " (foo, bar, zzz) values");
        int size = 4;
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append(String.format("(%d, %d, '%d')", i, i * 2, i * 3));
        }

        var tm = createTransactionManagerOcc(getSession());
        var detail = tm.execute(transaction -> {
            try (var ps = transaction.getSession().createStatement(sql.toString())) {
                return transaction.executeAndGetCountDetail(ps);
            }
        });
        assertEquals(size, detail.getInsertedCount());
        assertEquals(size, detail.getTotalCount());

        var list = selectAllFromTest();
        assertEquals(size, list.size());
        for (int i = 0; i < size; i++) {
            var actual = list.get(i);
            assertEquals(i, actual.getFoo());
            assertEquals((long) i * 2, actual.getBar());
            assertEquals(Integer.toString(i * 3), actual.getZzz());
        }
    }

    @Test
    void insertMultipleValues_bingVariable() throws Exception {
        var sql = new StringBuilder("insert into " + TEST + " (foo, bar, zzz) values");
        var variables = TgBindVariables.of();
        var parameter = TgBindParameters.of();

        int size = 4;
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append(String.format("(:foo%d, :bar%d, :zzz%d)", i, i, i));

            variables.addInt("foo" + i);
            parameter.addInt("foo" + i, i);
            variables.addLong("bar" + i);
            parameter.addLong("bar" + i, i * 2);
            variables.addString("zzz" + i);
            parameter.addString("zzz" + i, Integer.toString(i * 3));
        }

        var tm = createTransactionManagerOcc(getSession());
        var detail = tm.execute(transaction -> {
            try (var ps = transaction.getSession().createStatement(sql.toString(), TgParameterMapping.of(variables))) {
                return transaction.executeAndGetCountDetail(ps, parameter);
            }
        });
        assertEquals(size, detail.getInsertedCount());
        assertEquals(size, detail.getTotalCount());

        var list = selectAllFromTest();
        assertEquals(size, list.size());
        for (int i = 0; i < size; i++) {
            var actual = list.get(i);
            assertEquals(i, actual.getFoo());
            assertEquals((long) i * 2, actual.getBar());
            assertEquals(Integer.toString(i * 3), actual.getZzz());
        }
    }
}
