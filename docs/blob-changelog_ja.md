# Iceaxe BLOB, CLOBに関する変更

## Iceaxe 1.16.0

BLOB中継サービスに対応。

- セッション接続時にLOB転送モード（特権モードかBLOB中継サービス利用モードか）を指定する。
  - 従来、LOB転送のデフォルトは特権モードだったが、BLOB中継サービスに変更。
- LOB転送を行うAPIとして、TsurugiLobFactoryを新設。
- select文の実行結果からBLOB, CLOBを取得するAPIには変更なし。
  - TgBlobReference, TgClobReferenceからTgBlob, TgClobを作る際に、一時ファイルだけでなく、byte[]やStringで保持できるオプション機能を追加。

### プリペアードステートメントのAPIの変更点

#### Iceaxe 1.16.0の新方式

TsurugiLobFactoryを使ってデータをアップロードし、返ってきたTgRemoteBlob, TgRemoteClobをプリペアードステートメントのパラメーターに使用する。

```java
TsurugiLobFactory lobFactory = session.getLobFactory();

// BLOBデータをアップロードし、TgRemoteBlobを取得
TgRemoteBlob blob = lobFactory.uploadBlob(inputStream);
var parameter = TgBindParameters.of()
    .addInt("pk", i)
    .addBlob("value", blob);

// SQL実行後にTgRemoteBlobはクローズされる
transaction.executeAndGetCountDetail(ps, parameter);
```

#### Pathを指定する方式

Iceaxe 1.8.0では、Pathを保持したTgBlob, TgClobを作り、それをパラメーターに指定していた。

Iceaxe 1.16.0でもそのAPIは使用できるが、内部的には、SQL実行時にTsurugiLobFactoryを使ってアップロードするよう変更になった。

```java
var blob = TgBlob.of(path); // Pathを保持したTgBlob
var parameter = TgBindParameters.of()
    .addInt("pk", i)
    .addBlob("value", blob);

// 内部では、SQL実行時にBLOBデータをアップロードする
transaction.executeAndGetCountDetail(ps, parameter);
```

#### SQL実行後にクローズするTgBlob, TgClob

Iceaxe 1.8.0では、IceaxeObjectFactoryを使って、SQL実行後にクローズされる（内部で作成した一時ファイルを削除する）TgBlob, TgClobを作ることができたが、これは非推奨となった。

```java
var objectFactory = IceaxeObjectFactory.getDefaultInstance();

// 一時ファイルを作成するTgBlob
try (TgBlob blob = objectFactory.createBlob(inputStream, true)) { // 非推奨化
    var parameter = TgBindParameters.of()
        .addInt("pk", i)
        .addBlob("value", blob);
    
    // 一時ファイルはSQL実行後に削除される
    transaction.executeAndGetCountDetail(ps, parameter);
}
```

TgBlob, TgClobはSQL実行後も再利用できるオブジェクトという位置付けになったため、SQL実行後にクローズする機能は将来削除する予定。

また、IceaxeObjectFactoryは基本的にIceaxe内部で使用するものとし、Iceaxeを利用するアプリケーションからはTsurugiLobFactoryを使用することとする。

SQL実行後にクローズするという目的では、TsurugiLobFactoryでアップロードして返るTgRemoteBlob, TgRemoteClobを使用する。

## Iceaxe 1.9.0

パスマッピング機能を追加。

## Iceaxe 1.8.0

特権モードに対するBLOB, CLOBに対応。