package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;

/**
 * select date test
 */
class DbSelectDateTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectDateTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTable();
        insertTable(4);

        LOG.debug("init end");
    }

    private static void createTable() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  value date" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insertTable(int size) throws IOException {
        var variable = TgVariable.ofDate("value");
        var sql = "insert into " + TEST + "(value) values (" + variable + ")";
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, mapping)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var date = LocalDate.of(2022, 10, 1 + i);
                    var parameter = TgParameterList.of(variable.bind(date));
                    transaction.executeAndGetCount(ps, parameter);
                }
            });
        }
    }

    @Test
    void whereEq() throws IOException {
        var variable = TgVariable.ofDate("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, mapping)) {
            var date = LocalDate.of(2022, 10, 2);
            var parameter = TgParameterList.of(variable.bind(date));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(date, entity.getDate("value"));
        }
    }

    @Test
    void whereRange() throws IOException {
        var start = TgVariable.ofDate("start");
        var end = TgVariable.ofDate("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, mapping)) {
            var parameter = TgParameterList.of(start.bind(LocalDate.of(2022, 10, 2)), end.bind(LocalDate.of(2022, 10, 3)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(LocalDate.of(2022, 10, 2), list.get(0).getDate("value"));
            assertEquals(LocalDate.of(2022, 10, 3), list.get(1).getDate("value"));
        }
    }
}
