# Iceaxe BLOB, CLOB使用方法

当文書では、BLOB, CLOBをIceaxeで使用する基本的な方法を説明します。

## はじめに

Tsurugiでは、（メモリーに載せるのが躊躇われるような）大きなサイズのデータをBLOB, CLOB型で扱います。
BLOB（binary large object）はバイト列、CLOB（character large object）は文字列が対象です。

TsurugiはインメモリーDBなので、基本的にデータは全てDBサーバーのメモリー上に置くのですが、BLOB, CLOBのデータはメモリー上には置かず、データごとに個別のファイルとして保存されます。

クライアント（Iceaxe）とTsurugi DBの間でBLOB, CLOBデータを受け渡す方法はいくつかあり、どの方法を使うかはクライアントアプリケーション（Iceaxeを利用するアプリケーション）側で指定することができますが、データを受け渡すAPI（コーディング方法）は同一です。

## LOB転送モード

Iceaxeでは、BLOB, CLOBを共通で扱う場合は large object（LOB）という用語を用います。

クライアント（Iceaxe）とTsurugi DBの間でLOBデータを受け渡す方法（LOB転送を行う方法）は、LOB転送モードと呼びます。

LOB転送モードには以下のようなものがあります。

### 特権モード

特権モードはIceaxe 1.8.0（Tsurugi 1.3.0）以降で使用できます。

特権モードでは、クライアントアプリケーションとTsurugi DBの間でのLOBデータの受け渡しをファイル経由で行います。  
すなわち、LOBデータをDBに登録する際は、ユーザー（クライアントアプリケーション）がLOBデータのファイルを用意し、そのファイルのパスをTsurugi DBに渡します（Tsurugi DBは渡されたパスのファイルをコピーします）。  
DBからLOBデータを取得する際も、DB内で保持されているファイルのパスが返るので、そのファイルを読むことになります（ユーザーに対しては、ファイルのパスは隠蔽されます）。

Iceaxeには、InputStreamやbyte[]を引数としてBLOBデータを受け渡すメソッド（CLOBではReaderやStringを引数とするメソッド）がありますが、特権モードの場合はIceaxe内部で一時ファイルを作成し、そのファイルのパスを受け渡します。

このように特権モードではLOBデータをファイルで受け渡しするため、クライアントアプリケーションはTsurugi DBと同じサーバー上で実行する必要があります。

なお、この方式が使えるのは、Tsurugi DBのエンドポイントが特権モードで稼働している場合のみです。  
（デフォルトでは、IPC接続は特権モードで稼働しています）

### BLOB中継サービス利用モード

BLOB中継サービスはIceaxe 1.16.0（Tsurugi 1.11.0）以降で使用できます。

BLOB中継サービスは、クライアントアプリケーションとTsurugi DBの間でのLOBデータの受け渡しをgRPC（TCP/IP）で行います。  
（このため、クライアントアプリケーションとTsurugi DBが異なるサーバー上で動いていても使用できます）

なお、この方式が使えるのは、BLOB中継サービスが稼働している場合のみです。
（Tsurugi 1.11.0では、デフォルトでBLOB中継サービスが稼働しています）

> [!NOTE]
>
> BLOB中継サービスはCLOBも扱います。（CLOB中継サービスというものはありません）

> [!TIP]
>
> 小さなデータであれば、LOB転送を使わずに、データを直接受け渡すこともできます。
>
> BLOB, CLOBカラムへの登録（insert）であれば、リテラルが使用できます。
> （BLOBは `X'十六進数'`、CLOBは `'文字列'` ）
>
> selectする際にvarbinaryやvarcharにキャストすれば、データを直接取得できます。
> （BLOBは `cast(blob_column as varbinary)`、CLOBは `cast(clob_column as varchar)` ）

## TgLobTransferType

どのLOB転送モードを使うのかは、セッション接続時にTgLobTransferType列挙型で指定します。

何も指定しなかった場合は `DEFAULT` として扱われます。

```java
import com.tsurugidb.iceaxe.session.TgLobTransferType;

var sessionOption = TgSessionOption.of();
sessionOption.setLobTransferType(TgLobTransferType.DEFAULT);
```

- `DEFAULT`
  - BLOB中継サービスを使用します。Tsurugi側でBLOB中継サービスが使用できなくても、セッション接続は成功します。
- `NOT_USE`
  - LOB転送を行いません。
- `PRIVILEGED`
  - 特権モードを使用します。Tsurugi側で特権モードが使用できない場合、セッション接続が失敗します。
- `RELAY`
  - BLOB中継サービスを使用します。Tsurugi側でBLOB中継サービスが使用できない場合、セッション接続が失敗します。

> [!NOTE]
>
> Iceaxe 1.16.0より前は、LOB転送のデフォルトは特権モードでした。
> （そもそも特権モードしか無かったため、LOB転送モードを指定することはできませんでした）
>
> Iceaxe 1.16.0で、LOB転送のデフォルトはBLOB中継サービスになりました。

また、セッション確立後に実際にどのLOB転送モードになったのかを、TsurugiSessionから取得することができます。

```java
var lobTransferType = session.getLobTransferType();
```

返ってくる型は（セッション接続時のオプションと同じく）TgLobTransferTypeですが、`DEFAULT` が返ることはありません。

## IceaxeにおけるBLOB, CLOBの扱い

Iceaxeでは、BLOB, CLOBを以下の表にあるクラスで扱います。

以降、当文書ではBLOBの説明のみを記載します。
IceaxeにおけるCLOBの操作方法はBLOBと同等ですので、CLOBについては以下の点を読み替えてください。

|                    | BLOB        | CLOB   |
| ------------------ | ----------- | ------ |
| TgDataType         | BLOB        | CLOB   |
| LOBデータを扱うクラス | TgBlob      | TgClob |
| LOBをプリペアードステートメントで扱うクラス | TgRemoteBlob | TgRemoteClob |
| LOBをselect結果から取得するクラス | TgBlobReference | TgClobReference |
| データを読むクラス    | InputStream | Reader |
| データの型           | byte[]      | String |

### BLOBを扱うクラスの違い

TgBlob, TgRemoteBlob, TgBlobReferenceには以下のような違いがあります。

- TgBlob
  - データをファイルまたはbyte[]で保持するBLOB
  - アプリケーションスコープ
  - 主にTsurugiResultEntityからBLOBを取得する際に使われる。
- TgRemoteBlob
  - プリペアードステートメントのパラメーターとして使用するためのBLOB
  - セッションスコープ
  - プリペアードステートメントを使ったSQLの実行や実行計画取得に使う。
- TgBlobReference
  - select文の実行結果として取得されるBLOB
  - トランザクションスコープ
  - TsurugiResultRecordからBLOBを取得する際に使われる。

## プリペアードステートメントでBLOBを使用する方法

プリペアードステートメント（TsurugiSqlPreparedStatementやTsurugiSqlPreparedQuery）でBLOBを扱う手順は以下のようになります。

1. TsurugiSessionからTsurugiLobFactoryを取得する。
2. TsurugiLobFactoryのアップロードメソッドを使ってBLOBデータをアップロードし、TgRemoteBlobを取得する。
3. TgRemoteBlobを使ってTgBindParameterを生成する。
4. TsurugiSqlPreparedStatementやTsurugiSqlPreparedQueryを使ったSQL実行メソッドや実行計画取得メソッドの引数にTgBindParameterを渡す。
   - SQL実行メソッドや実行計画取得メソッドの中でTgRemoteBlobはクローズされる。
   - SQL実行メソッドや実行計画取得メソッドで使われなかった場合、TgRemoteBlobはTsurugiSessionクローズ時にクローズされる。

TsurugiLobFactoryのアップロードメソッドは、利便性のために、BLOBファイルのPathやInputStream, byte[]を引数で受け取るメソッドが用意されています。

### 特権モードの注意

特権モードで引数がBLOBファイルのPathのアップロードメソッドを使う場合、そのファイルはSQL実行や実行計画取得時点でも存在している必要があります。

また、引数がInputStreamやbyte[]のアップロードメソッドは、特権モードの場合、メソッド内部で一時ファイルを作成します。  
この一時ファイルはTgRemoteBlobがクローズされる際に削除されます。

## select文の実行結果からBLOBを取得する方法

select文の実行結果の値は、TsurugiResultRecordまたはTsurugiResultEntityから取得することができます。

TsurugiResultRecordからBLOBを取得すると、TgBlobReferenceが返ります。  
TgBlobReferenceにBLOBデータを取得するメソッドがありますが、TgBlobReferenceは **トランザクションが有効な間のみ** 使用可能であるという制約があります。

トランザクションの外でもBLOBデータを保持し続けるEntityクラス（TsurugiResultEntityやユーザーが用意するEntityクラス）向けには、TgBlobが使用されます。  
（TsurugiResultEntityからBLOBを取得するとTgBlobが返ります）

TgBlob内部では、一時ファイルやbyte[]の形でBLOBデータを保持します。（デフォルトは一時ファイルを使う方式です）

TgBlob内部で一時ファイルを使ってBLOBデータを保持している場合、TgBlobをクローズするとその一時ファイルは削除されます。  
（逆に言えば、TgBlobをクローズしないと一時ファイルが削除されず、リークすることになります）

このように、EntityクラスでもBLOBデータを扱うことはできるのですが、実行効率はトランザクション内で直接TgBlobReferenceを使うよりも悪くなります。

### TgBlob内のデータ保持方式を変更する方法

TsurugiResultEntityのためのTgBlobは、TgBlobReferenceを元に、TsurugiLobFactoryを使って生成されます。

TsurugiLobFactoryでは、TgBlobがBLOBデータを保持する方式をTgLobPersistenceType列挙型で指定できるようになっており、デフォルトは `FILE` です。

- `FILE`
  - 一時ファイルを作成し、BLOBデータをそのファイルで保持する。
  - メモリー使用量は少ないが、ファイルアクセスする分、実行速度が遅い。
  - TgBlobをクローズしないと、一時ファイルは削除されない。
- `MEMORY`
  - BLOBデータをbyte[]で保持する。
  - メモリー使用量が多くなる危険性がある。

TsurugiLobFactoryのsetDefaultPersistenceTypeメソッドで、デフォルトの保持方式を設定できます。

```java
var lobFactory = session.getLobFactory();
lobFactory.setDefaultPersistenceType(TgLobPersistenceType.MEMORY);
```

> [!TIP]
>
> Iceaxe全体の設定は、IceaxeObjectFactoryのsetDefaultPersistenceTypeメソッドで変更することができます。
> （TsurugiLobFactoryは内部でIceaxeObjectFactoryを呼んでいます）
>
> 一時ファイルが作られる場所は、システムプロパティー `java.io.tmpdir` で示されるディレクトリーの下です。
> それ以外の場所に変更したい場合は、IceaxeObjectFactoryのsetTempDirectoryメソッドで指定してください。
>
> なお、ここで指定するディレクトリーには、クライアントアプリケーション（Iceaxe）からファイルを作成して読み書きできる権限と、特権モードの場合はTsurugi DBプロセスからの読み取り権限が必要です。

## IceaxeにおけるBLOBの例

IceaxeでBLOBを使用する例は [iceaxe-examplesの例](https://github.com/project-tsurugi/iceaxe/blob/master/modules/iceaxe-examples/src/main/java/com/tsurugidb/iceaxe/example/Example72Blob.java) をご覧ください。

最も基本的な例を以下に示します。

### insertの例

```java
void insert(TsurugiTransactionManager tm) throws IOException, InterruptedException {
    var sql = "insert into blob_example values(:pk, :value)";
    var variables = TgBindVariables.of().addInt("pk").addBlob("value");
    var parameterMapping = TgParameterMapping.of(variables);

    try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
        List<TgRemoteBlob> blobList;
        try (var stream = Files.list(Path.of("dir"))) { // ファイル一覧を取得
            var lobFactory = tm.getSession().getLobFactory();

            // 各ファイルをアップロード
            blobList = stream
                .map(path -> lobFactory.uploadBlob(path)) // 実際は例外処理が必要
                .collect(Collectors.toList());
        }

        tm.execute(transaction -> {
            int i = 0;
            for (TgRemoteBlob blob : blobList) {
                var parameter = TgBindParameters.of()
                    .addInt("pk", i++)
                    .addBlob("value", blob);
                transaction.executeAndGetCountDetail(ps, parameter);
            }
        });
    }
}
```

TgRemoteBlobはSQL実行時に（executeAndGetCountDetailメソッド内で）クローズされます。
使用されなかった場合はTsurugiSessionクローズ時にクローズされます。

### selectの例

```java
void select(TsurugiTransactionManager tm) throws IOException, InterruptedException {
    var sql = "select pk, value from blob_example order by pk";
    var resultMapping = TgResultMapping.of(record -> record);

    try (var ps = tm.getSession().createQuery(sql, resultMapping)) {
        tm.execute(transaction -> {
            transaction.executeAndForEach(ps, record -> { // TsurugiResultRecord
                try (TgBlobReference blob = record.getBlob("value")) {
                    try (InputStream is = blob.openInputStream()) {
                        // isからデータを読む
                    }
                }
            });
        });
    }
}
```

TgBlobReferenceは、使用後にクローズする必要があります。
明示的にクローズしなかった場合は、トランザクションクローズ時にクローズされます。

### 特権モードのパスマッピングの例

Iceaxe 1.9.0で、特権モードでLOBファイルを扱う際にクライアント側のパスとサーバー側のパスを変換する機能が導入されました。

特権モードでLOBファイルを扱う場合、クライアントとサーバーが同一ファイルシステムにアクセスできることを前提としていますが、環境によってはクライアントとサーバーのパスが一致しないことがあります。  
このとき、パスマッピングを設定することで、LOBファイルのパスをクライアントからサーバーに送信する際にクライアント側のパスがサーバー側のパスに変換されます。同様に、サーバーからファイルのパスを受信した際にクライアント側のパスに変換されます。

例えば以下のようにMS-Windows上のTsurugiのDockerでボリュームマウントしてIceaxeでパスマッピングを指定すると、`D:/tmp/client/blob.bin` のBLOBファイルをinsertすることができます。  
また、`D:/tmp/tsurugi` の下にあるBLOBファイルを読むという扱いでselectすることができます。（selectする際のファイルパスはユーザーからは隠蔽されますが）

```bash
docker run -d -p 12345:12345 --name tsurugi -v D:/tmp/client:/mnt/client -v D:/tmp/tsurugi:/opt/tsurugi/var/data/log -e GLOG_v=30 ghcr.io/project-tsurugi/tsurugidb:latest
```

```java
        var connector = TsurugiConnector.of("tcp://localhost:12345");
        var sessionOption = TgSessionOption.of()
            .setLobTransferType(TgLobTransferType.PRIVILEGED) // 特権モード
            .addLargeObjectPathMappingOnSend(Path.of("D:/tmp/client"), "/mnt/client")
            .addLargeObjectPathMappingOnReceive("/opt/tsurugi/var/data/log", Path.of("D:/tmp/tsurugi"));
        try (var session = connector.createSession(sessionOption)) {
            ～
        }
```

