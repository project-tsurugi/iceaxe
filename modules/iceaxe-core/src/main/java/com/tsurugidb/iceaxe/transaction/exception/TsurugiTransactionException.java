package com.tsurugidb.iceaxe.transaction.exception;

import java.util.Optional;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
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

    private int iceaxeTxId;
    private int iceaxeTmExecuteId;
    private int attempt;
    private TgTxOption txOption;
    private TgTxMethod txMethod;
    private int iceaxeTxExecuteId;
    private TsurugiSql sqlStatement;
    private Object sqlParameter;

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
     * set sql.
     *
     * @param transaction       transaction
     * @param method            transaction method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @param ps                SQL statement
     * @param parameter         SQL parameter
     */
    public void setSql(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter) {
        this.iceaxeTxId = transaction.getIceaxeTxId();
        this.iceaxeTmExecuteId = transaction.getIceaxeTmExecuteId();
        this.attempt = transaction.getAttempt();
        this.txOption = transaction.getTransactionOption();
        this.txMethod = method;
        this.iceaxeTxExecuteId = iceaxeTxExecuteId;
        this.sqlStatement = ps;
        this.sqlParameter = parameter;
    }

    @Override
    public int getIceaxeTxId() {
        return this.iceaxeTxId;
    }

    @Override
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
    }

    @Override
    public int getAttempt() {
        return this.attempt;
    }

    @Override
    public TgTxOption getTransactionOption() {
        return this.txOption;
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
    public Optional<TsurugiTransactionException> findTransactionException() {
        return Optional.of(this);
    }
}
