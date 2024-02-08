package com.tsurugidb.iceaxe.sql;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi SQL definition (not prepared).
 */
public abstract class TsurugiSqlDirect extends TsurugiSql {

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize()} after construct.
     * </p>
     *
     * @param session session
     * @param sql     SQL
     */
    @IceaxeInternal
    protected TsurugiSqlDirect(TsurugiSession session, String sql) {
        super(session, sql);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @throws IOException if session already closed
     * @since X.X.X
     */
    @IceaxeInternal
    @Override
    public void initialize() throws IOException {
        super.initialize();
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
