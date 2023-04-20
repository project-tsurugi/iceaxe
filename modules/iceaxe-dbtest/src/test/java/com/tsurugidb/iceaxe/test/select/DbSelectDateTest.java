package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select date test
 */
class DbSelectDateTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectDateTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTable();
        insertTable(4);

        LOG.debug("init end");
    }

    private static void createTable() throws IOException, InterruptedException {
        var sql = "create table " + TEST //
                + "(" //
                + "  value date" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insertTable(int size) throws Exception {
        var variable = TgBindVariable.ofDate("value");
        var sql = "insert into " + TEST + "(value) values (" + variable + ")";
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, mapping)) {
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var date = LocalDate.of(2022, 10, 1 + i);
                    var parameter = TgBindParameters.of(variable.bind(date));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    @Test
    void whereEq() throws Exception {
        var variable = TgBindVariable.ofDate("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var date = LocalDate.of(2022, 10, 2);
            var parameter = TgBindParameters.of(variable.bind(date));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(date, entity.getDate("value"));
        }
    }

    @Test
    void whereRange() throws Exception {
        var start = TgBindVariable.ofDate("start");
        var end = TgBindVariable.ofDate("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(LocalDate.of(2022, 10, 2)), end.bind(LocalDate.of(2022, 10, 3)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(LocalDate.of(2022, 10, 2), list.get(0).getDate("value"));
            assertEquals(LocalDate.of(2022, 10, 3), list.get(1).getDate("value"));
        }
    }
}
