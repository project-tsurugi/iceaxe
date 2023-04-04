package com.tsurugidb.iceaxe.transaction.exception;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi Transaction Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionException extends Exception implements TsurugiDiagnosticCodeProvider {

    private TsurugiTransaction transaction;
    private TgTxMethod txMethod;
    private int iceaxeTxExecuteId;
    private TsurugiSql sqlStatement;
    private Object sqlParameter;
    private TsurugiSqlResult sqlResult;

    // internal
    public TsurugiTransactionException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    /**
     * Creates a new instance.
     *
     * @param message the detail message
     * @param code    diagnostic code
     * @param cause   the cause
     */
    public TsurugiTransactionException(String message, DiagnosticCode code, Throwable cause) {
        super(message, new ServerException(TsurugiDiagnosticCodeProvider.createMessage(code), cause) {
            @Override
            public DiagnosticCode getDiagnosticCode() {
                return code;
            }
        });
    }

    @Override
    public ServerException getCause() {
        return (ServerException) super.getCause();
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        return getCause().getDiagnosticCode();
    }

    /**
     * set SQL.
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    public void setSql(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TsurugiSqlResult result) {
        this.transaction = transaction;
        this.sqlStatement = ps;
        this.sqlParameter = parameter;
        this.sqlResult = result;
    }

    /**
     * set transaction method.
     *
     * @param method            transaction method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     */
    public void setTxMethod(TgTxMethod method, int iceaxeTxExecuteId) {
        this.txMethod = method;
        this.iceaxeTxExecuteId = iceaxeTxExecuteId;
    }

    @Override
    public int getIceaxeTxId() {
        return (transaction != null) ? transaction.getIceaxeTxId() : 0;
    }

    @Override
    public int getIceaxeTmExecuteId() {
        return (transaction != null) ? transaction.getIceaxeTmExecuteId() : 0;
    }

    @Override
    public int getAttempt() {
        return (transaction != null) ? transaction.getAttempt() : 0;
    }

    @Override
    public @Nullable TgTxOption getTransactionOption() {
        return (transaction != null) ? transaction.getTransactionOption() : null;
    }

    @Override
    public @Nullable String getTransactionId() {
        try {
            return (transaction != null) ? transaction.getTransactionId() : null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public TgTxMethod getTxMethod() {
        return this.txMethod;
    }

    @Override
    public int getIceaxeTxExecuteId() {
        return this.iceaxeTxExecuteId;
    }

    @Override
    public @Nullable TsurugiSql getSqlStatement() {
        return this.sqlStatement;
    }

    @Override
    public @Nullable Object getSqlParameter() {
        return this.sqlParameter;
    }

    @Override
    public int getIceaxeSqlExecuteId() {
        return (sqlResult != null) ? sqlResult.getIceaxeSqlExecuteId() : 0;
    }

    @Override
    public Optional<TsurugiTransactionException> findTransactionException() {
        return Optional.of(this);
    }
}
