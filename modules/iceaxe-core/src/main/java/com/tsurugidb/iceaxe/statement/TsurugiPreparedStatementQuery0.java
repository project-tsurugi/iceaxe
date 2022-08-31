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
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementQuery0.class);

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
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        var lowTransaction = transaction.getLowTransaction();
        LOG.trace("executeQuery start");
        var lowResultSetFuture = lowTransaction.executeQuery(sql);
        LOG.trace("executeQuery started");
        var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
        var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping, convertUtil);
        return result;
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        try (var rs = execute(transaction)) {
            return rs.findRecord();
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
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return record
     * @throws IOException
     */
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.execute(setting, transaction -> {
            return executeAndFindRecord(transaction);
        });
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public List<R> executeAndGetList(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        try (var rs = execute(transaction)) {
            return rs.getRecordList();
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
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return list of record
     * @throws IOException
     */
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.execute(setting, transaction -> {
            return executeAndGetList(transaction);
        });
    }
}
