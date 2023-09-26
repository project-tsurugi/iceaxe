package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * explain insert test
 */
class DbInsertExplainTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbInsertExplainTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void pareparedStatement() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 2, '3')";

        var session = getSession();
        try (var ps = session.createStatement(sql)) {
            assertThrowsExactly(UnsupportedOperationException.class, () -> {
                var result = ps.explain();
                assertExplain(result);
            }); // TODO explain実装待ち
        }
    }

    @Test
    void psParameter() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var entity = createTestEntity(1);
            var result = ps.explain(entity);
            assertExplain(result);
        }
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertNotNull(actual.getLowPlanGraph());

        var list = actual.getLowColumnList();
        assertEquals(0, list.size());
    }
}
