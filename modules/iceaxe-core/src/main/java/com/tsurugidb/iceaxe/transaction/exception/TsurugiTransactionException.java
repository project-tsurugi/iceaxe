package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi Transaction Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionException extends Exception implements TsurugiDiagnosticCodeProvider {

    // internal
    public TsurugiTransactionException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    @Override
    public ServerException getCause() {
        return (ServerException) super.getCause();
    }

    @Override
    public DiagnosticCode getLowDiagnosticCode() {
        return getCause().getDiagnosticCode();
    }
}
