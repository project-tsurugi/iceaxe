package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select cursor fetch test
 */
class DbSelectFetchTest extends DbTestTableTester {

    private static final int SIZE = 8300;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TEST2);
        createTest2Table();

        LOG.debug("init end");
    }

    private static final String TEST2 = "test2";

    private static void createTest2Table() throws IOException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    @Test
    void fetch() throws IOException {
        var cond = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo=" + cond;
        var parameterMapping = TgParameterMapping.of(cond);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery("select foo from " + TEST); //
                var ps2 = session.createPreparedQuery(sql, parameterMapping, SELECT_MAPPING); //
                var ps3 = session.createPreparedStatement(INSERT_SQL.replace(TEST, TEST2), INSERT_MAPPING)) {
            tm.execute(transaction -> {
                transaction.executeForEach(ps, fetch -> {
                    int foo = fetch.getInt4("foo");
                    var parameter = TgParameterList.of(cond.bind(foo));
                    var entity = transaction.executeAndFindRecord(ps2, parameter).get();
                    transaction.executeAndGetCount(ps3, entity);
                });
            });
            assertEquals(SIZE, selectCountFrom(TEST2));
        }
    }
}
