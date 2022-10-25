package com.tsurugidb.iceaxe.exception;

import java.util.Optional;

import javax.annotation.Nullable;

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
}
