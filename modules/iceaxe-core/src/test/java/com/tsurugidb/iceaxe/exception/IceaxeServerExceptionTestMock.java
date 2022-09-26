package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

@SuppressWarnings("serial")
public class IceaxeServerExceptionTestMock extends ServerException {

    private final DiagnosticCode code;

    public IceaxeServerExceptionTestMock(String message, DiagnosticCode code) {
        super(message);
        this.code = code;
    }

    public IceaxeServerExceptionTestMock(String message, int codeNumber) {
        this(message, new IceaxeDiagnosticCodeTestMock(codeNumber));
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        return this.code;
    }
}
