package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;

/**
 * update test
 */
class DbUpdate2Test extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();
        insertTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  pk int primary key," //
                + "  value1 int," //
                + "  value2 int" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insertTable() throws IOException {
        var sql = "insert into " + TEST //
                + "(pk, value1, value2)" //
                + "values(:pk, :value1, :value2)";
        var pk = TgVariable.ofInt4("pk");
        var v1 = TgVariable.ofInt4("value1");
        var v2 = TgVariable.ofInt4("value2");
        var parameterMapping = TgParameterMapping.of(pk, v1, v2);

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < SIZE; i++) {
                    var param = TgParameterList.of(pk.bind(i), v1.bind(initValue1(i)), v2.bind(initValue2(i)));
                    ps.executeAndGetCount(transaction, param);
                }
            });
        }
    }

    private static int initValue1(int i) {
        return i + 100;
    }

    private static int initValue2(int i) {
        return i * 2;
    }

    @Test
    void update() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  value1 = value1 + 1," //
                + "  value2 = value2 + value1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var selectSql = "select * from " + TEST + " order by pk";
        try (var ps = session.createPreparedQuery(selectSql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            int i = 0;
            for (var entity : list) {
                int v1 = initValue1(i);
                int v2 = initValue2(i);
                assertEquals(i, entity.getInt4("pk"));
                assertEquals(v1 + 1, entity.getInt4("value1"));
                assertEquals(v2 + v1, entity.getInt4("value2"));
                i++;
            }
        }
    }

    @Test
    void swap() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  value1 = value2," //
                + "  value2 = value1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var selectSql = "select * from " + TEST + " order by pk";
        try (var ps = session.createPreparedQuery(selectSql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            int i = 0;
            for (var entity : list) {
                int v1 = initValue1(i);
                int v2 = initValue2(i);
                assertEquals(i, entity.getInt4("pk"));
                assertEquals(v2, entity.getInt4("value1"));
                assertEquals(v1, entity.getInt4("value2"));
                i++;
            }
        }
    }
}
