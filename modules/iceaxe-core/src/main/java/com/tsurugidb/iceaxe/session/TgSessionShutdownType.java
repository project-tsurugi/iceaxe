/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
