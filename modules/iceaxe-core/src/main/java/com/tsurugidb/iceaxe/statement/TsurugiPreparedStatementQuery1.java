package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

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
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementQuery1.class);

    private final TgResultMapping<R> resultMapping;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, String sql, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping,
            TgResultMapping<R> resultMapping) {
        super(session, sql, lowPreparedStatementFuture, parameterMapping);
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
        checkClose();

        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);
        LOG.trace("executeQuery start");
        var lowResultSetFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeQuery(lowPs, lowParameterList));
        LOG.trace("executeQuery started");
        var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
        var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping, convertUtil);
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
     * @param tm        Transaction Manager
     * @param setting   transaction manager settings
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.execute(setting, transaction -> {
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
     * @param tm        Transaction Manager
     * @param setting   transaction manager settings
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.execute(setting, transaction -> {
            return executeAndGetList(transaction, parameter);
        });
    }
}
