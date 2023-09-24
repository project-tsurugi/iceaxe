# Iceaxe - Java High-level-API

[![javadoc](https://javadoc.io/badge2/com.tsurugidb.iceaxe/iceaxe-core/javadoc.svg)](https://javadoc.io/doc/com.tsurugidb.iceaxe/iceaxe-core)

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * tsubakuro

## How to build

### Build with Tsubakuro that deployed GitHub Packages

First, set up the following credentials for the GitHub Packages, and build nomally.
* Gradle property `gpr.user` or environment variable `GPR_USER` with your GitHub username
* Gradle Property `gpr.key` or environment variable `GPR_KEY` with your personal access token

Ref: [iceaxe.libs-conventions.gradle](buildSrc/src/main/groovy/iceaxe.libs-conventions.gradle)

```
cd iceaxe
./gradlew build
```

### Build with Tsubakuro that installed locally

First, check out and install Tsubakuro locally, and build Iceaxe with Gradle Property `mavenLocal` .

```bash
cd tsubakuro
./gradlew publishMavenJavaPublicationToMavenLocal

cd iceaxe
./gradlew build -PmavenLocal
```

## How to use

To use on Gradle, add Iceaxe library to dependencies.

```
dependencies {
    api 'com.tsurugidb.iceaxe:iceaxe-core:1.0.0-SNAPSHOT'
}
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
