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
    static final int TRANSACTION_MANAGER = 3000;
    static final int TRANSACTION = 4000;
    static final int STATEMENT = 5000;
    static final int RESULT = 6000;
    static final int EXPLAIN = 7000;
    static final int METADATA = 8000;

    private IceaxeErrorCodeBlock() {
    }
}
