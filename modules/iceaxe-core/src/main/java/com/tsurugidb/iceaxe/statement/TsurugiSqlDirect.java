package com.tsurugidb.iceaxe.statement;

import java.io.IOException;

import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * Tsurugi SQL (not prepared) statement
 */
public class TsurugiSqlDirect extends TsurugiSql {

    protected TsurugiSqlDirect(TsurugiSession session, String sql) throws IOException {
        super(session, sql);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @return statement metadata
     * @throws IOException
     */
    public TgStatementMetadata explain() throws IOException {
        var session = getSession();
        var helper = session.getExplainHelper();
        return helper.explain(session, sql, getExplainConnectTimeout(), getExplainCloseTimeout());
    }
}
