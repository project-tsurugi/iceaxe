package com.tsurugidb.iceaxe.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class TsurugiIOExceptionTest {

    @Test
    void constructMessageIOException() {
        var se = new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_LOW_ERROR);
        var target = new TsurugiIOException("test1", new IOException(se.getMessage(), se));
        assertEquals("test1", target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructServerException() {
        var target = new TsurugiIOException(new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_LOW_ERROR));
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.name() + ": test", target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructCode() {
        var target = new IceaxeIOException(IceaxeErrorCode.TX_LOW_ERROR);
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.getMessage(), target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructCodeIOException() {
        var se = new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_ALREADY_CLOSED);
        var target = new IceaxeIOException(IceaxeErrorCode.TX_LOW_ERROR, new IOException(se.getMessage(), se));
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.getMessage() + ": " + se.getMessage(), target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }
}
