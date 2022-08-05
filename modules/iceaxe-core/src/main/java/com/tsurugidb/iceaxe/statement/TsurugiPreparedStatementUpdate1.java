package com.tsurugidb.iceaxe.statement;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
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
 * <li>TODO+++翻訳: SQLのパラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 */
public class TsurugiPreparedStatementUpdate1<P> extends TsurugiPreparedStatementWithLowPs<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementUpdate1.class);

    // internal
    public TsurugiPreparedStatementUpdate1(TsurugiSession session, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) {
        super(session, lowPreparedStatementFuture, parameterMapping);
    }

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return result
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        var lowTransaction = transaction.getLowTransaction();
        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);
        LOG.trace("executeStatement");
        var lowResultFuture = lowTransaction.executeStatement(lowPs, lowParameterList);
        var result = new TsurugiResultCount(transaction, lowResultFuture);
        return result;
    }

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int executeAndGetCount(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        try (var result = execute(transaction, parameter)) {
            return result.getUpdateCount();
        }
    }

    /**
     * execute statement
     * 
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.execute(transaction -> {
            return executeAndGetCount(transaction, parameter);
        });
    }

    /**
     * execute statement
     * 
     * @param tm        Transaction Manager
     * @param setting   transaction manager setting
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.execute(setting, transaction -> {
            return executeAndGetCount(transaction, parameter);
        });
    }
}
