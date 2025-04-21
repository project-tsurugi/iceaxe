# Iceaxe BLOB, CLOB使用方法

当文書では、BLOB, CLOBをIceaxeで使用する基本的な方法を説明します。

## はじめに

Tsurugiでは、（メモリーに載せるのが躊躇われるような）大きなサイズのデータをBLOB, CLOB型で扱います。
BLOB（binary large object）はバイト列、CLOB（character large object）は文字列が対象です。

TsurugiはインメモリーDBなので、基本的にデータは全てDBサーバーのメモリー上に置くのですが、BLOB, CLOBのデータはメモリー上には置かず、データごとに個別のファイルとして保存されます。

そして、BLOB, CLOBデータをクライアントアプリケーションとTsurugi DBの間で受け渡すのもファイル経由で行います。  
すなわち、BLOB, CLOBデータをDBに登録する際は、ユーザー（クライアントアプリケーション）がBLOB, CLOBデータのファイルを用意し、そのファイルのパスをTsurugi DBに渡します（Tsurugi DBは渡されたパスのファイルをコピーします）。  
DBからBLOB, CLOBデータを取得する際も、DB内で保持されているファイルのパスが返るので、そのファイルを読むことになります（実際は、ファイルのパスは隠蔽されます）。

このようにBLOB, CLOBデータはファイルで受け渡しするため、BLOB, CLOBを扱うクライアントアプリケーションはTsurugi DBと同じサーバー上で実行する必要があります。

なお、BLOB, CLOBを扱えるのはTsurugi DBのエンドポイントが特権モードで稼働している場合のみです。  
（デフォルトでは、IPC接続は特権モードで稼働しています）

Iceaxeもこれらの制約を受けるため、BLOB, CLOBデータをファイルで扱います。

> [!NOTE]
>
> 小さなデータを登録するのであれば、リテラル（BLOBは `X'十六進数'`、CLOBは `'文字列'` ）が使用できます。（この場合はデータを渡す際にファイルを使用しません）
>
> また、selectする際にvarbinaryやvarcharにキャストすれば（ファイルを経由せずに）データを直接取得できます。（BLOBは `cast(blob_column as varbinary)`、CLOBは `cast(clob_column as varchar)` ）


## IceaxeにおけるBLOB, CLOBの扱い

Iceaxeでは、BLOBデータを扱うクラスとしてTgBlob、CLOBデータを扱うクラスとしてTgClobを用意しています。

以降、当文書ではBLOBの説明のみを記載します。
IceaxeにおけるCLOBの操作方法はBLOBと同等ですので、CLOBについては以下の点を読み替えてください。

|                    | BLOB        | CLOB   |
| ------------------ | ----------- | ------ |
| TgDataType         | BLOB        | CLOB   |
| データを扱うクラス    | TgBlob      | TgClob |
| データを読むクラス    | InputStream | Reader |
| データの型           | byte[]      | String |

BLOBをDBに登録するために使うTgBlobの実体は、Path（ユーザーが用意するファイルのパス）を保持するクラスとなります。  
また、利便性のために、InputStreamやbyte[]を受け取って一時ファイルを作成し、そのパスを保持するクラスもあります。

BLOBを取得するselect文では、実行結果としてファイルのパスが返ってきます。
ただし、そのファイルを読むことができるのは **トランザクションが有効な間のみ** という制約があります。  
そのために、TgBlobとは別に、トランザクション内でのみ使用可能なTgBlobReferenceというクラスを用意しました。

TsurugiResultRecordはトランザクション内でselect結果を扱うクラスなので、DBから返されたパスのファイルをそのまま読むことができます。
（TsurugiResultRecordからBLOBを取得するとTgBlobReferenceが返ります）

しかし、トランザクションの外でもデータを保持し続けるEntityクラス（TsurugiResultEntityやユーザーが用意するEntityクラス）向けには、一時ファイルを作成してDBのファイルをコピーします。
（TsurugiResultEntityからBLOBを取得するとTgBlobが返ります）  
このため、EntityクラスでもBLOBデータを扱うことはできるのですが、実行速度はトランザクション内で直接扱うよりも遅くなります。



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
        List<Path> fileList;
        try (var stream = Files.list(Path.of("dir"))) {
            fileList = stream.collect(Collectors.toList());
        }

        tm.execute(transaction -> {
            int i = 0;
            for (Path path : fileList) {
                var blob = TgBlob.of(path); // TgBlobインスタンスの生成

                var parameter = TgBindParameters.of()
                    .addInt("pk", i++)
                    .addBlob("value", blob);
                transaction.executeAndGetCountDetail(ps, parameter);
            }
        });
    }
}
```

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

### パスマッピングの例

Iceaxe 1.9.0では、BLOB/CLOBファイルを扱うために、クライアント側のパスとサーバー側のパスを変換する機能があります。

BLOB/CLOBファイルを扱う場合、クライアントとサーバーが同一ファイルシステムにアクセスできることを前提としていますが、環境によってはクライアントとサーバーのパスが一致しないことがあります。  
このとき、パスマッピングを設定することで、BLOB/CLOBファイルのパスをクライアントからサーバーに送信する際にクライアント側のパスがサーバー側のパスに変換されます。同様に、サーバーからファイルのパスを受信した際にクライアント側のパスに変換されます。

例えば以下のようにTsurugiのDockerでボリュームマウントしてIceaxeでパスマッピングを指定すると、`D:/tmp/client/blob.bin` のBLOBファイルをinsertすることができます。  
また、`D:/tmp/tsurugi` の下にあるBLOBファイルを読むという扱いでselectすることができます。（selectする際のファイルパスはユーザーからは隠蔽されますが）

```bash
docker run -d -p 12345:12345 --name tsurugi -v D:/tmp/client:/mnt/client -v D:/tmp/tsurugi:/opt/tsurugi/var/data/log -e GLOG_v=30 ghcr.io/project-tsurugi/tsurugidb:latest
```

```java
        var connector = TsurugiConnector.of("tcp://localhost:12345");
        var sessionOption = TgSessionOption.of()
            .addLargeObjectPathMappingOnSend(Path.of("D:/tmp/client"), "/mnt/client")
            .addLargeObjectPathMappingOnReceive("/opt/tsurugi/var/data/log", Path.of("D:/tmp/tsurugi"));
        try (var session = connector.createSession(sessionOption)) {
            ～
        }
```





## BLOBの一時ファイルについて

Iceaxeでは、利便性のために、InputStreamやbyte[]で渡されたBLOBデータから一時ファイルを作成してそのパスを保持する機能を提供しています。

一時ファイルのパスを保持しているTgBlobをクローズすると、一時ファイルを削除します。

### インスタンスの生成方法

#### 一時ファイルを作成するTgBlob

InputStreamやbyte[]からTgBlobインスタンスを生成するには、IceaxeObjectFactoryクラスを使用します。
この場合、内部で一時ファイルが作成されます。

```java
var objectFactory = IceaxeObjectFactory.getDefaultInstance();
try (TgBlob blob = objectFactory.createBlob(inputStream, false)) {
    var parameter = TgBindParameters.of() //
        .addInt("pk", i) //
        .addBlob("value", blob);
    transaction.executeAndGetCountDetail(ps, parameter);
}
```

createBlob()の第1引数にはbyte[]を渡すことも可能です。

createBlob()の第2引数は、TsurugiTransactionのexecute系メソッド実行後に その中でTgBlobをクローズするかどうかを指定します。  
trueであればexecute系メソッド内でクローズするのでblobに対するtry文は不要になりますが、try文が無い状態でexecute系メソッドを呼ばなかったら、closeが呼ばれず一時ファイルが残り続けることになります。

その他に、表面上IceaxeObjectFactoryでTgBlobを生成せずにInputStreamやbyte[]を渡せるメソッドもありますが、内部ではIceaxeObjectFactory.getDefaultInstance()を用いてTgBlobインスタンスを生成しています。

#### TgBlobReferenceからTgBlobへの変換

TsurugiResultRecordから返される、トランザクション内でselect結果のBLOBデータを扱うクラスがTgBlobReferenceです。
BLOBデータをトランザクションの外に出す（Entityクラスにセットする）際には、データをTgBlobReferenceから一時ファイルにコピーしてTgBlobを生成します。
この際にIceaxeObjectFactoryが使われます。

TsurugiResultRecordは、IceaxeConvertUtilという型変換ユーティリティーのインスタンスを保持しています。（TgResultMappingまたはTsurugiSessionでセットされたIceaxeConvertUtilです）  
IceaxeConvertUtilにはIceaxeObjectFactoryをセットできますので、デフォルト以外のIceaxeObjectFactoryを使いたい場合は、ここにセットしてください。

### IceaxeObjectFactory

IceaxeObjectFactoryは、「一時ファイルを保持するTgBlob」を生成するファクトリークラスです。

一時ファイルを作成する場所は、デフォルトではシステムプロパティー `java.io.tmpdir` で示されるディレクトリーの下です。
それ以外の場所に変更したい場合は、IceaxeObjectFactory#setTempDirectory()で指定してください。

なお、ここで指定するディレクトリーには、クライアントアプリケーション（Iceaxe）からファイルを作成して読み書きできる権限と、Tsurugi DBプロセスからの読み取り権限が必要です。

> [!NOTE]
>
> 一時ファイルを作成するディレクトリーを指定しておけば、一時ファイルの削除漏れがあっても、後から一括でディレクトリーごと削除することができます。（むしろ、毎回一時ファイルを削除するよりも、後から一括で削除する方が効率的かもしれません）