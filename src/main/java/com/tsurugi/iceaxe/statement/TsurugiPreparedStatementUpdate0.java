package com.tsurugi.iceaxe.statement;

import java.io.IOException;

import com.tsurugi.iceaxe.result.TsurugiResultCount;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: 更新系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターなし</li>
 * </ul>
 */
public class TsurugiPreparedStatementUpdate0 extends TsurugiPreparedStatement {

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
     * @throws TsurugiTransactionIOException
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction) throws TsurugiTransactionIOException {
        try {
            var lowTransaction = transaction.getLowTransaction();
            var lowResultFuture = lowTransaction.executeStatement(sql);
            var result = new TsurugiResultCount(transaction, lowResultFuture);
            return result;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }
}
