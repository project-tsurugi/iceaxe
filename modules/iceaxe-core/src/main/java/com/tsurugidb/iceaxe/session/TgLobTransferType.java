/*
 * Copyright 2023-2026 Project Tsurugi.
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

import java.text.MessageFormat;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.common.BlobTransferType;

/**
 * Tsurugi Large Object Transfer Type.
 *
 * @since 1.16.0
 */
public enum TgLobTransferType {

    /**
     * Indicates the default transfer policy.
     */
    DEFAULT(BlobTransferType.DEFAULT),

    /**
     * Does not use transfer type.
     */
    NOT_USE(BlobTransferType.DOES_NOT_USE),

    /**
     * Privileged transfer type.
     */
    PRIVILEGED(BlobTransferType.PRIVILEGED),

    /**
     * Blob Relay transfer type.
     */
    RELAY(BlobTransferType.RELAY),

    //
    ;

    private final BlobTransferType lowLobTransferType;

    private TgLobTransferType(BlobTransferType lowLobTransferType) {
        this.lowLobTransferType = lowLobTransferType;
    }

    /**
     * get {@link BlobTransferType}.
     *
     * @return large object transfer type
     */
    @IceaxeInternal
    public @Nonnull BlobTransferType getLowLobTransferType() {
        return this.lowLobTransferType;
    }

    /**
     * get {@link TgLobTransferType} from {@link BlobTransferType}.
     *
     * @param lowLobTransferType low level large object transfer type
     * @return large object transfer type
     * @throws IllegalArgumentException if the specified low level large object transfer type is unknown
     */
    @IceaxeInternal
    public static TgLobTransferType valueOf(BlobTransferType lowLobTransferType) {
        for (TgLobTransferType tgLargeObjectTransferType : values()) {
            if (tgLargeObjectTransferType.getLowLobTransferType() == lowLobTransferType) {
                return tgLargeObjectTransferType;
            }
        }
        throw new IllegalArgumentException(MessageFormat.format("Unknown BlobTransferType: {0}", lowLobTransferType));
    }
}
