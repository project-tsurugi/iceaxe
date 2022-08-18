package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * PreparedStatement execute error test
 */
class DbErrorPsExecuteTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach() throws IOException {
        dropTestTable();
        createTestTable();
    }

    @Test
    @Disabled // TODO remove Disabled
    void intsertByExecuteQuery() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + entity.getFoo() + ", " + entity.getBar() + ", '" + entity.getZzz() + "')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            ps.executeAndGetList(tm);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    @Disabled // TODO remove Disabled
    void selectByExecuteStatement() throws IOException {
        int size = 4;
        insertTestTable(size);

        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(size, count);
        }
    }
}
