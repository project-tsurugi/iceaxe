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
package com.tsurugidb.iceaxe.metadata;

import java.util.Objects;

/**
 * Int with Arbitrary.
 */
public final /* record */ class ArbitraryInt {

    /**
     * Creates a new instance.
     *
     * @param value value
     * @return instance
     */
    public static ArbitraryInt of(int value) {
        return new ArbitraryInt(value, false);
    }

    private static final ArbitraryInt ARBITRARY = new ArbitraryInt(0, true);

    /**
     * Get arbitrary instance.
     *
     * @return instance
     */
    public static ArbitraryInt ofArbitrary() {
        return ARBITRARY;
    }

    private final int value;
    private final boolean arbitrary;

    private ArbitraryInt(int value, boolean arbitrary) {
        this.value = value;
        this.arbitrary = arbitrary;
    }

    /**
     * Arbitrary exception.
     */
    @SuppressWarnings("serial")
    public static class ArbitraryException extends RuntimeException {
        /**
         * Creates a new instance.
         */
        public ArbitraryException() {
            super("arbitrary");
        }
    }

    /**
     * Get value.
     *
     * @return value
     * @throws ArbitraryException if arbitrary
     */
    public int value() throws ArbitraryException {
        if (this.arbitrary) {
            throw new ArbitraryException();
        }
        return this.value;
    }

    /**
     * Get arbitrary.
     *
     * @return arbitrary
     */
    public boolean arbitrary() {
        return this.arbitrary;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, arbitrary);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArbitraryInt) {
            var that = (ArbitraryInt) obj;

            if (this.arbitrary || that.arbitrary) {
                return this.arbitrary == that.arbitrary;
            }
            return this.value == that.value;
        }

        return false;
    }

    @Override
    public String toString() {
        if (this.arbitrary) {
            return "*";
        }
        return Integer.toString(this.value);
    }
}
