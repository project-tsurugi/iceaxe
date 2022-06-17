package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: クエリー系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 * @param <R> result type
 */
public class TsurugiPreparedStatementQuery1<P, R> extends TsurugiPreparedStatementWithLowPs<P> {

    private final TgResultMapping<R> resultMapping;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping,
            TgResultMapping<R> resultMapping) {
        super(session, lowPreparedStatementFuture, parameterMapping);
        this.resultMapping = resultMapping;
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return Result Set
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        var lowTransaction = transaction.getLowTransaction();
        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);
        var lowResultSetFuture = lowTransaction.executeQuery(lowPs, lowParameterList);
        var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping);
        return result;
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        try (var rs = execute(transaction, parameter)) {
            return rs.findRecord();
        }
    }

    /**
     * execute query
     * 
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.execute(transaction -> {
            return executeAndFindRecord(transaction, parameter);
        });
    }

    /**
     * execute query
     * 
     * @param tm                        Transaction Manager
     * @param parameter                 SQL parameter
     * @param transactionOptionSupplier transaction option
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTxOptionSupplier transactionOptionSupplier, P parameter) throws IOException {
        return tm.execute(transactionOptionSupplier, transaction -> {
            return executeAndFindRecord(transaction, parameter);
        });
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public List<R> executeAndGetList(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        try (var rs = execute(transaction, parameter)) {
            return rs.getRecordList();
        }
    }

    /**
     * execute query
     * 
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.execute(transaction -> {
            return executeAndGetList(transaction, parameter);
        });
    }

    /**
     * execute query
     * 
     * @param tm                        Transaction Manager
     * @param transactionOptionSupplier transaction option
     * @param parameter                 SQL parameter
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTxOptionSupplier transactionOptionSupplier, P parameter) throws IOException {
        return tm.execute(transactionOptionSupplier, transaction -> {
            return executeAndGetList(transaction, parameter);
        });
    }
}
