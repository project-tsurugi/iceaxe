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

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;

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
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertConstant(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + " values(" + entity.getFoo() + ", " + entity.getBar() + ", '" + entity.getZzz() + "')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertEquals(-1, count); // TODO 1
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByVariableList(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + "values(:foo, :bar, :zzz)";
        var vlist = TgVariableList.of() //
                .int4("foo") //
                .int8("bar") //
                .character("zzz");
        var parameterMapping = TgParameterMapping.of(vlist);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var plist = TgParameterList.of() //
                    .int4("foo", entity.getFoo()) //
                    .int8("bar", entity.getBar()) //
                    .character("zzz", entity.getZzz());
            int count = tm.executeAndGetCount(ps, plist);
            assertEquals(-1, count); // TODO 1
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByBind(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var zzz = TgVariable.ofCharacter("zzz");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var plist = TgParameterList.of( //
                    foo.bind(entity.getFoo()), //
                    bar.bind(entity.getBar()), //
                    zzz.bind(entity.getZzz()));
            int count = tm.executeAndGetCount(ps, plist);
            assertEquals(-1, count); // TODO 1
        }

        assertEqualsTestTable(entity);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByEntity(boolean columns) throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + (columns ? " (" + TEST_COLUMNS + ")" : "") //
                + "values(:foo, :bar, :zzz)";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .int4("foo", TestEntity::getFoo) //
                .int8("bar", TestEntity::getBar) //
                .character("zzz", TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            int count = tm.executeAndGetCount(ps, entity);
            assertEquals(-1, count); // TODO 1
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
                .int4("foo", TestEntity::getFoo) //
                .int8("bar", TestEntity::getBar) //
                .character("zzz", TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (var entity : entityList) {
                    int count = transaction.executeAndGetCount(ps, entity);
                    assertEquals(-1, count); // TODO 1
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
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);

                    if (resultCheck) {
                        try (var rc = ps.execute(transaction, entity)) {
                            var count = rc.getUpdateCount();
                            assertEquals(-1, count); // TODO 1
                        }
                    } else {
                        @SuppressWarnings("unused")
                        var rc = ps.execute(transaction, entity);
                        // rc.close is called on transaction.close
                    }
                }
            });
        }

        assertEqualsTestTable(size);
    }

    @Test
    void insertPart() throws IOException {
        int key = 1;
        var foo = TgVariable.ofInt4("foo");
        var sql = "insert into " + TEST + "(foo) values(" + foo + ")";
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var parameter = TgParameterList.of(foo.bind(key));
            int count = tm.executeAndGetCount(ps, parameter);
            assertEquals(-1, count); // TODO 1
        }

        var actual = selectFromTest(key);
        assertEquals(key, actual.getFoo());
        assertNull(actual.getBar());
        assertNull(actual.getZzz());
    }
}
