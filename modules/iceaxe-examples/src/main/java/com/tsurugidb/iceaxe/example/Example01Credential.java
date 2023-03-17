package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Credential example
 */
public class Example01Credential {

    public static Credential getCredential() {
        return getNullCredential();
    }

    static Credential getNullCredential() {
        return NullCredential.INSTANCE;
    }

    static Credential getUserPasswordCredential() {
        return new UsernamePasswordCredential("user", "password");
    }

    static Credential getTokenCredential() {
        return new RememberMeCredential("token");
    }

    static Credential getFileCredential() {
        var file = Path.of("/path/to/credential.json");
        try {
            return FileCredential.load(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
