package com.tsurugidb.iceaxe.exception;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;

@SuppressWarnings("serial")
public class IceaxeServerExceptionTestMock extends ServerException {

    private final DiagnosticCode code;

    public IceaxeServerExceptionTestMock(String message) {
        super(message);
        this.code = SqlServiceCode.OK;
    }

    public IceaxeServerExceptionTestMock(String message, String code) {
        super(message);
        this.code = new IceaxeDiagnosticCodeTestMock(code);
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        return this.code;
    }
}