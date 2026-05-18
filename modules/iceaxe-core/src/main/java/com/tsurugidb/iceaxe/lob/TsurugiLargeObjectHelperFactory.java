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
package com.tsurugidb.iceaxe.lob;

import java.text.MessageFormat;

import com.tsurugidb.iceaxe.session.TgLobTransferType;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.type.TsurugiLobFactory;

/**
 * {@link TsurugiLargeObjectHelper} Factory.
 *
 * @since 1.16.0
 */
public class TsurugiLargeObjectHelperFactory {

    /**
     * Create a {@link TsurugiLargeObjectHelper} based on the specified {@link TgLobTransferType}.
     * 
     * @param lobTransferType the large object transfer type
     * @return the created {@link TsurugiLargeObjectHelper}
     */
    public TsurugiLargeObjectHelper createHelper(TgLobTransferType lobTransferType) {
        switch (lobTransferType) {
        case NOT_USE:
            return new TsurugiNotUseLargeObjectHelper();
        case PRIVILEGED:
            return new TsurugiPrivilegedLargeObjectHelper();
        case RELAY:
            return new TsurugiRelayLargeObjectHelper();
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported large object transfer type: {0}", lobTransferType));
        }
    }

    /**
     * Create a {@link TsurugiLobFactory}.
     * 
     * @param session the Tsurugi session
     * @return the created {@link TsurugiLobFactory}
     */
    public TsurugiLobFactory createLobFactory(TsurugiSession session) {
        return new TsurugiLobFactory(session);
    }
}
