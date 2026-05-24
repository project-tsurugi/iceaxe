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
package com.tsurugidb.iceaxe.sql.parameter;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi low parameter generation context.
 *
 * @since 1.16.0
 */
@IceaxeInternal
public final /* record */ class IceaxeLowParameterGenerateContext {

    private final TsurugiSession session;
    private final IceaxeConvertUtil convertUtil;
    private final IceaxeCloseableSet closeableSet;

    /**
     * Creates a new instance.
     *
     * @param session      session
     * @param convertUtil  convert util
     * @param closeableSet closeable set
     */
    public IceaxeLowParameterGenerateContext(TsurugiSession session, IceaxeConvertUtil convertUtil, IceaxeCloseableSet closeableSet) {
        this.session = session;
        this.convertUtil = convertUtil;
        this.closeableSet = closeableSet;
    }

    /**
     * get session.
     *
     * @return session
     */
    public TsurugiSession session() {
        return session;
    }

    /**
     * get convert util.
     *
     * @return convert util
     */
    public IceaxeConvertUtil convertUtil() {
        return convertUtil;
    }

    /**
     * get closeable set.
     *
     * @return closeable set
     */
    public IceaxeCloseableSet closeableSet() {
        return closeableSet;
    }
}
