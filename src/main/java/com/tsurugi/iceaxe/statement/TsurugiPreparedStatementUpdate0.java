package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.List;

import com.tsurugi.iceaxe.result.TsurugiResultCount;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TgTransactionOption;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionManager;

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

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @return row count
     * @throws TsurugiTransactionIOException
     */
    public int executeAndGetCount(TsurugiTransaction transaction) throws TsurugiTransactionIOException {
        try (var result = execute(transaction)) {
            return result.getUpdateCount();
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
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
     * @param tm                    Transaction Manager
     * @param transactionOptionList transaction option
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, List<TgTransactionOption> transactionOptionList) throws IOException {
        return tm.execute(transactionOptionList, transaction -> {
            return executeAndGetCount(transaction);
        });
    }
}
