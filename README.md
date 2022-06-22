# Iceaxe - Java High-level-API

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * tsubakuro

## How to build

```bash
cd tsubakuro
./gradlew publishMavenJavaPublicationToMavenLocal

cd iceaxe
./gradlew build
```

## How to use

To use on Gradle, add Iceaxe library to dependencies.

```
dependencies {
    api 'com.tsurugidb.iceaxe:iceaxe-core:0.0.1-SNAPSHOT'
}
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
