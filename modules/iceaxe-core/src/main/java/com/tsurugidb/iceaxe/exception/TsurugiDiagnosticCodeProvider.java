package com.tsurugidb.iceaxe.exception;

import java.util.Optional;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * DiagnosticCode provider
 */
public interface TsurugiDiagnosticCodeProvider {

    /**
     * get DiagnosticCode
     *
     * @return DiagnosticCode (null if not found)
     */
    @Nullable
    public DiagnosticCode getDiagnosticCode();

    // utility

    /**
     * get DiagnosticCode provider
     *
     * @param e Throwable
     * @return DiagnosticCode provider
     */
    public static Optional<TsurugiDiagnosticCodeProvider> findDiagnosticCodeProvider(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TsurugiDiagnosticCodeProvider) {
                return Optional.of((TsurugiDiagnosticCodeProvider) t);
            }
            if (t instanceof ServerException) {
                var serverException = (ServerException) t;
                return Optional.of(new TsurugiDiagnosticCodeProvider() {
                    @Override
                    public DiagnosticCode getDiagnosticCode() {
                        return serverException.getDiagnosticCode();
                    }
                });
            }
        }
        return Optional.empty();
    }

    /**
     * create message
     *
     * @param e ServerException
     * @return message
     */
    @Nullable
    public static String createMessage(ServerException e) {
        var code = e.getDiagnosticCode();
        if (code != null) {
            return code.name() + ": " + e.getMessage();
        } else {
            return e.getMessage();
        }
    }

    /**
     * create message
     *
     * @param code DiagnosticCode
     * @return message
     */
    @Nullable
    public static String createMessage(DiagnosticCode code) {
        if (code == null) {
            return null;
        }
        return code.toString();
    }

    // transaction information

    /**
     * get iceaxe transactionId
     *
     * @return iceaxe transactionId
     */
    public default int getIceaxeTxId() {
        return findTransactionException().map(e -> e.getIceaxeTxId()).orElse(0);
    }

    /**
     * get iceaxe tm executeId
     *
     * @return iceaxe tm executeId
     */
    public default int getIceaxeTmExecuteId() {
        return findTransactionException().map(e -> e.getIceaxeTmExecuteId()).orElse(0);
    }

    /**
     * get attempt number
     *
     * @return attempt number
     * @see TgTmTxOptionSupplier#get(int, TsurugiTransactionException)
     */
    public default int getAttempt() {
        return findTransactionException().map(e -> e.getAttempt()).orElse(0);
    }

    /**
     * get transaction option
     *
     * @return transaction option
     */
    public default TgTxOption getTransactionOption() {
        return findTransactionException().map(e -> e.getTransactionOption()).orElse(null);
    }

    // transaction execute information

    /**
     * get transaction method.
     *
     * @return transaction method
     */
    public default @Nullable TgTxMethod getTxMethod() {
        return findTransactionException().map(e -> e.getTxMethod()).orElse(null);
    }

    /**
     * get iceaxe tx executeId.
     *
     * @return iceaxe tx executeId
     */
    public default int getIceaxeTxExecuteId() {
        return findTransactionException().map(e -> e.getIceaxeTxExecuteId()).orElse(0);
    }

    /**
     * get SQL statement.
     *
     * @return SQL statement
     */
    public default @Nullable TsurugiSql getSqlStatement() {
        return findTransactionException().map(e -> e.getSqlStatement()).orElse(null);
    }

    /**
     * get SQL parameter.
     *
     * @return SQL parameter
     */
    public default @Nullable Object getSqlParameter() {
        return findTransactionException().map(e -> e.getSqlParameter()).orElse(null);
    }

    /**
     * get transaction exception.
     *
     * @return transaction exception
     */
    public default Optional<TsurugiTransactionException> findTransactionException() {
        for (var t = (Throwable) this; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return Optional.of((TsurugiTransactionException) t);
            }
        }
        return Optional.empty();
    }
}
