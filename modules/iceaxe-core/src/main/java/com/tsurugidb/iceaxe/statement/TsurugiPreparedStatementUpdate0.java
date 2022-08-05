package com.tsurugidb.iceaxe.statement;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: 更新系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターなし</li>
 * </ul>
 */
public class TsurugiPreparedStatementUpdate0 extends TsurugiPreparedStatement {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementUpdate0.class);

    private final String sql;

    // internal
    public TsurugiPreparedStatementUpdate0(TsurugiSession session, String sql) {
        super(session);
        this.sql = sql;
    }

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @return result
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        var lowTransaction = transaction.getLowTransaction();
        LOG.trace("executeStatement");
        var lowResultFuture = lowTransaction.executeStatement(sql);
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
    public int executeAndGetCount(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        try (var result = execute(transaction)) {
            return result.getUpdateCount();
        }
    }

    /**
     * execute statement
     * 
     * @param tm Transaction Manager
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm) throws IOException {
        return tm.execute(transaction -> {
            return executeAndGetCount(transaction);
        });
    }

    /**
     * execute statement
     * 
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.execute(setting, transaction -> {
            return executeAndGetCount(transaction);
        });
    }
}
