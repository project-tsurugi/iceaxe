package com.tsurugidb.iceaxe.sql;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;

/**
 * Tsurugi SQL (not prepared) statement
 */
public abstract class TsurugiSqlDirect extends TsurugiSql {

    protected TsurugiSqlDirect(TsurugiSession session, String sql) throws IOException {
        super(session, sql);
    }

    @Override
    public final boolean isPrepared() {
        return false;
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
