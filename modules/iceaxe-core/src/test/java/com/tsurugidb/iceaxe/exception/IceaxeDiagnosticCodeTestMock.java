package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

public class IceaxeDiagnosticCodeTestMock implements DiagnosticCode {

    private final int number;

    public IceaxeDiagnosticCodeTestMock(int number) {
        this.number = number;
    }

    @Override
    public String getStructuredCode() {
        return String.format("TEST-%05d", getCodeNumber());
    }

    @Override
    public int getCodeNumber() {
        return this.number;
    }

    @Override
    public String name() {
        return "MOCK_" + number;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getStructuredCode(), name());
    }
}
