package com.tsurugidb.iceaxe.session;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.common.ShutdownType;

/**
 * Tsurugi Session Shutdown Type.
 *
 * @since 1.4.0
 */
public enum TgSessionShutdownType {

    /**
     * No shutdown.
     */
    NOTHING(null),

    /**
     * Waits for the ongoing requests and safely shutdown the session.
     */
    GRACEFUL(ShutdownType.GRACEFUL),

    /**
     * Cancelling the ongoing requests and safely shutdown the session.
     */
    FORCEFUL(ShutdownType.FORCEFUL),

    //
    ;

    private final ShutdownType lowShutdownType;

    private TgSessionShutdownType(ShutdownType lowShutdownType) {
        this.lowShutdownType = lowShutdownType;
    }

    /**
     * get {@link ShutdownType}.
     *
     * @return shutdown type
     */
    @IceaxeInternal
    public @Nullable ShutdownType getLowShutdownType() {
        return this.lowShutdownType;
    }
}
