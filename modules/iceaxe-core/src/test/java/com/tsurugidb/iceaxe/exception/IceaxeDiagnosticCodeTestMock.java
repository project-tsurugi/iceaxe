package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

public class IceaxeDiagnosticCodeTestMock implements DiagnosticCode {

    private final String message;

    public IceaxeDiagnosticCodeTestMock(String message) {
        this.message = message;
    }

    @Override
    public String getStructuredCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCodeNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return this.message;
    }
}
