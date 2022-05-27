package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTransactionOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: クエリー系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターなし</li>
 * </ul>
 * 
 * @param <R> result type
 */
public class TsurugiPreparedStatementQuery0<R> extends TsurugiPreparedStatement {

    private final String sql;
    private final TgResultMapping<R> resultMapping;

    // internal
    public TsurugiPreparedStatementQuery0(TsurugiSession session, String sql, TgResultMapping<R> resultMapping) {
        super(session);
        this.sql = sql;
        this.resultMapping = resultMapping;
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @return Result Set
     * @throws TsurugiTransactionIOException
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction) throws TsurugiTransactionIOException {
        try {
            var lowTransaction = transaction.getLowTransaction();
            var lowResultSetFuture = lowTransaction.executeQuery(sql);
            var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping);
            return result;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @return record
     * @throws TsurugiTransactionIOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction) throws TsurugiTransactionIOException {
        try (var rs = execute(transaction)) {
            return rs.findRecord();
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }

    /**
     * execute query
     * 
     * @param tm Transaction Manager
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm) throws IOException {
        return tm.execute(transaction -> {
            return executeAndFindRecord(transaction);
        });
    }

    /**
     * execute query
     * 
     * @param tm                    Transaction Manager
     * @param transactionOptionList transaction option
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, List<TgTransactionOption> transactionOptionList) throws IOException {
        return tm.execute(transactionOptionList, transaction -> {
            return executeAndFindRecord(transaction);
        });
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @return list of record
     * @throws TsurugiTransactionIOException
     */
    public List<R> executeAndGetList(TsurugiTransaction transaction) throws TsurugiTransactionIOException {
        try (var rs = execute(transaction)) {
            return rs.getRecordList();
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }

    /**
     * execute query
     * 
     * @param tm Transaction Manager
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm) throws IOException {
        return tm.execute(transaction -> {
            return executeAndGetList(transaction);
        });
    }

    /**
     * execute query
     * 
     * @param tm                    Transaction Manager
     * @param transactionOptionList transaction option
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm, List<TgTransactionOption> transactionOptionList) throws IOException {
        return tm.execute(transactionOptionList, transaction -> {
            return executeAndGetList(transaction);
        });
    }
}
