package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select cursor fetch test
 */
class DbSelectFetchTest extends DbTestTableTester {

    private static final int SIZE = 8300;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectFetchTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TEST2);
        createTest2Table();

        logInitEnd(LOG, info);
    }

    private static final String TEST2 = "test2";

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    @Test
    void fetch() throws Exception {
        var cond = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo=" + cond;
        var parameterMapping = TgParameterMapping.of(cond);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery("select foo from " + TEST); //
                var ps2 = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var ps3 = session.createStatement(INSERT_SQL.replace(TEST, TEST2), INSERT_MAPPING)) {
            tm.execute(transaction -> {
                transaction.executeAndForEach(ps, fetch -> {
                    int foo = fetch.getInt("foo");
                    var parameter = TgBindParameters.of(cond.bind(foo));
                    var entity = transaction.executeAndFindRecord(ps2, parameter).get();
                    transaction.executeAndGetCount(ps3, entity);
                });
            });
            assertEquals(SIZE, selectCountFrom(TEST2));
        }
    }
}
