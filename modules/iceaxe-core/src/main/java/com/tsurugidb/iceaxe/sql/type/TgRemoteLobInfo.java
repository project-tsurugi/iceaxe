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
package com.tsurugidb.iceaxe.sql.type;

import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;

/**
 * Tsurugi uploaded large object with low-level LargeObjectInfo.
 *
 * @since 1.16.0
 */
@IceaxeInternal
public abstract class TgRemoteLobInfo implements IceaxeTimeoutCloseable {

    private final LargeObjectInfo lowLargeObjectInfo;
    private IceaxeCloseableSet ownerCloseableSet = null;

    /**
     * Creates a new instance.
     *
     * @param lowLargeObjectInfo low-level LargeObjectInfo
     */
    public TgRemoteLobInfo(LargeObjectInfo lowLargeObjectInfo) {
        this.lowLargeObjectInfo = lowLargeObjectInfo;
    }

    /**
     * Gets low-level LargeObjectInfo.
     *
     * @return low-level LargeObjectInfo
     */
    public LargeObjectInfo getLowLargeObjectInfo() {
        return this.lowLargeObjectInfo;
    }

    /**
     * Sets the owner IceaxeCloseableSet.
     *
     * @param ownerCloseableSet owner IceaxeCloseableSet
     */
    public void setOwner(IceaxeCloseableSet ownerCloseableSet) {
        this.ownerCloseableSet = ownerCloseableSet;
        ownerCloseableSet.add(this);
    }

    @Override
    public void close() throws IOException {
        close(0);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close(long timeoutNanos) throws IOException {
        var closeableSet = this.ownerCloseableSet;
        if (closeableSet != null) {
            this.ownerCloseableSet = null;
            closeableSet.remove(this);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + lowLargeObjectInfo + ")";
    }
}
