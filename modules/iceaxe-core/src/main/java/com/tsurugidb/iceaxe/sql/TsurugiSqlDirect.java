package com.tsurugidb.iceaxe.sql;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;

/**
 * Tsurugi SQL definition (not prepared).
 */
public abstract class TsurugiSqlDirect extends TsurugiSql {

    /**
     * Creates a new instance.
     *
     * @param session session
     * @param sql     SQL
     * @throws IOException if an I/O error occurs while disposing the resources
     */
    protected TsurugiSqlDirect(TsurugiSession session, String sql) throws IOException {
        super(session, sql);
        initialize(null);
    }

    @Override
    public final boolean isPrepared() {
        return false;
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain() throws IOException, InterruptedException {
        var session = getSession();
        var helper = session.getExplainHelper();
        return helper.explain(session, sql, getExplainConnectTimeout(), getExplainCloseTimeout());
    }
}
