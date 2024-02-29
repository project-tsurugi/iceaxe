package com.tsurugidb.iceaxe.exception;

/**
 * Iceaxe diagnostic code block.
 *
 * @see IceaxeErrorCode
 */
public final class IceaxeErrorCodeBlock {

    static final int COMMON = 0;
    static final int CONNECTOR = 1000;
    static final int SESSION = 2000;
    static final int TRANSACTION = 3000;
    static final int STATEMENT = 4000;
    static final int RESULT = 5000;
    static final int EXPLAIN = 6000;
    static final int METADATA = 7000;

    private IceaxeErrorCodeBlock() {
    }
}
