package com.tsurugidb.iceaxe.statement;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: 更新系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターなし</li>
 * </ul>
 */
public class TsurugiPreparedStatementUpdate0 extends TsurugiPreparedStatement {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementUpdate0.class);

    // internal
    public TsurugiPreparedStatementUpdate0(TsurugiSession session, String sql) throws IOException {
        super(session, sql);
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @return result
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeStatement(TsurugiPreparedStatementUpdate0)
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        checkClose();

        LOG.trace("executeStatement start");
        var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(sql));
        LOG.trace("executeStatement started");
        var result = new TsurugiResultCount(transaction, lowResultFuture);
        return result;
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        return transaction.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm Transaction Manager
     * @return row count
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm) throws IOException {
        return tm.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return row count
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.executeAndGetCount(setting, this);
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
