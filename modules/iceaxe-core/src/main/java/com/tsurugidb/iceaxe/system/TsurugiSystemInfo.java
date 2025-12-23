/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.system;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.system.proto.SystemResponse.SystemInfo;

/**
 * Tsurugi system info.
 *
 * @since 1.13.0
 */
public class TsurugiSystemInfo {

    private final SystemInfo lowSystemInfo;

    /**
     * Creates a new instance.
     *
     * @param lowSystemInfo low system info
     */
    @IceaxeInternal
    public TsurugiSystemInfo(SystemInfo lowSystemInfo) {
        this.lowSystemInfo = lowSystemInfo;
    }

    /**
     * Get tsurugidb name.
     *
     * @return version
     */
    public String getName() {
        return lowSystemInfo.getName();
    }

    /**
     * Get tsurugidb version.
     *
     * @return version
     */
    public String getVersion() {
        return lowSystemInfo.getVersion();
    }

    /**
     * Get low system info.
     *
     * @return low system info
     */
    @IceaxeInternal
    public SystemInfo getLowSystemInfo() {
        return this.lowSystemInfo;
    }
}
