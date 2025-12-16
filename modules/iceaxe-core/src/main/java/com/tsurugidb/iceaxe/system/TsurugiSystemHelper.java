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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.system.proto.SystemResponse.SystemInfo;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.system.SystemClient;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi system helper.
 *
 * @since X.X.X
 */
public class TsurugiSystemHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSystemHelper.class);

    /**
     * Get system info.
     *
     * @param session tsurugi session
     * @return system info
     * @throws IOException          if an I/O error occurs while retrieving system info
     * @throws InterruptedException if interrupted while retrieving system info
     */
    public TsurugiSystemInfo getSystemInfo(TsurugiSession session) throws IOException, InterruptedException {
        var sessionOption = session.getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);

        LOG.trace("getSystemInfo start");
        try (var lowSystemClient = getLowSystemClient(session)) {
            var lowSystemInfoFuture = getLowSystemInfo(lowSystemClient);
            LOG.trace("getSystemInfo started");
            return getSystemInfo(lowSystemInfoFuture, connectTimeout);
        } catch (ServerException e) {
            throw new TsurugiIOException(e);
        }
    }

    /**
     * Get connect timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.SYSTEM_INFO_CONNECT);
    }

    /**
     * Get low system client.
     *
     * @param session tsurugi session
     * @return low system client
     * @throws IOException          if an I/O error occurs while communicating to the server
     * @throws InterruptedException if interrupted while communicating to the server
     */
    protected SystemClient getLowSystemClient(TsurugiSession session) throws IOException, InterruptedException {
        return SystemClient.attach(session.getLowSession());
    }

    /**
     * Get low system info.
     *
     * @param lowSystemClient low system client
     * @return future of system info
     * @throws IOException if an I/O error occurs while retrieving system info
     */
    protected FutureResponse<SystemInfo> getLowSystemInfo(SystemClient lowSystemClient) throws IOException {
        return lowSystemClient.getSystemInfo();
    }

    /**
     * Get system info.
     *
     * @param lowSystemInfoFuture future of low system info
     * @param connectTimeout      connect timeout
     * @return system info
     * @throws IOException          if an I/O error occurs while retrieving system info
     * @throws InterruptedException if interrupted while retrieving system info
     */
    protected TsurugiSystemInfo getSystemInfo(FutureResponse<SystemInfo> lowSystemInfoFuture, IceaxeTimeout connectTimeout) throws IOException, InterruptedException {
        var lowSystemInfo = IceaxeIoUtil.getAndCloseFuture(lowSystemInfoFuture, //
                connectTimeout, IceaxeErrorCode.SYSTEM_INFO_CONNECT_TIMEOUT, //
                IceaxeErrorCode.SYSTEM_INFO_CLOSE_TIMEOUT);
        LOG.trace("getSystemInfo end");

        return newSystemInfo(lowSystemInfo);
    }

    /**
     * Creates a new system info instance.
     *
     * @param lowSystemInfo low system info
     * @return system info
     */
    protected TsurugiSystemInfo newSystemInfo(SystemInfo lowSystemInfo) {
        return new TsurugiSystemInfo(lowSystemInfo);
    }
}
