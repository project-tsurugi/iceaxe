package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * insert (table without primary key) test
 */
class DbInsertNoPkTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        // no primary key
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertConstant(boolean columns) throws IOException {
        new DbInsertTest().insertConstant(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByVariableList(boolean columns) throws IOException {
        new DbInsertTest().insertByVariableList(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByBind(boolean columns) throws IOException {
        new DbInsertTest().insertByBind(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true/* , false */ }) // TODO without columns
    void insertByEntity(boolean columns) throws IOException {
        new DbInsertTest().insertByEntity(columns);
    }

    @Test
    void insertMany() throws IOException {
        new DbInsertTest().insertMany();
    }

    @Test
    void insertResultCheck() throws IOException {
        new DbInsertTest().insertResultCheck();
    }

    @Test
    void insertResultNoCheck() throws IOException {
        new DbInsertTest().insertResultNoCheck();
    }

    @Test
    void insertDuplicate() throws IOException {
        var entity = new TestEntity(123, 456, "abc");
        int size = 4;

        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + ", " + entity.getBar() + ", '" + entity.getZzz() + "')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            for (int i = 0; i < size; i++) {
                tm.execute(transaction -> {
                    int count = ps.executeAndGetCount(transaction);
                    assertEquals(-1, count); // TODO 1
                });
            }
        }

        var actualList = selectAllFromTest();
        assertEquals(size, actualList.size());
        for (var actual : actualList) {
            assertEquals(entity, actual);
        }
    }
}