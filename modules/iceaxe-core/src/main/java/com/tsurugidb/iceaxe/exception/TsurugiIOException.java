package com.tsurugidb.iceaxe.exception;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi IOException
 */
@SuppressWarnings("serial")
public class TsurugiIOException extends IOException implements TsurugiDiagnosticCodeProvider {

    private Optional<DiagnosticCode> diagnosticCode;

    /**
     * Creates a new instance.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    @IceaxeInternal
    public TsurugiIOException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     *
     * @param cause the cause
     */
    @IceaxeInternal
    public TsurugiIOException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    /**
     * Creates a new instance.
     *
     * @param code the diagnostic code
     */
    @IceaxeInternal
    public TsurugiIOException(IceaxeErrorCode code) {
        this(code, null);
    }

    /**
     * Creates a new instance.
     *
     * @param code  the diagnostic code
     * @param cause the cause
     */
    @IceaxeInternal
    public TsurugiIOException(IceaxeErrorCode code, Throwable cause) {
        super(code.getMessage(), cause);
        this.diagnosticCode = Optional.of(code);
    }

    @Override
    public @Nullable DiagnosticCode getDiagnosticCode() {
        if (this.diagnosticCode == null) {
            this.diagnosticCode = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(getCause()).map(e -> e.getDiagnosticCode());
        }
        return diagnosticCode.orElse(null);
    }
}
