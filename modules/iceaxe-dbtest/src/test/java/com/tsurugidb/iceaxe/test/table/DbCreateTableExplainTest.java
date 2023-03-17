package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * explain create table test
 */
class DbCreateTableExplainTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";

    @Test
    void create() throws Exception {
        var session = getSession();
        var helper = session.getExplainHelper();
        assertThrowsExactly(UnsupportedOperationException.class, () -> {
            var result = helper.explain(session, SQL);
            assertExplain(result);
        }); // TODO explain実装待ち
    }

    @Test
    void createExists() throws Exception {
        createTestTable();

        var session = getSession();
        var helper = session.getExplainHelper();
        assertThrowsExactly(UnsupportedOperationException.class, () -> {
            var result = helper.explain(session, SQL);
            assertExplain(result);
        }); // TODO explain実装待ち
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertThrowsExactly(PlanGraphException.class, () -> {
            actual.getLowPlanGraph();
        });

        var list = actual.getLowColumnList();
        assertEquals(0, list.size());
    }
}
