# Iceaxe内部構造

Iceaxeの内部構造の意図（抜粋）について説明する。



## クラス名の命名ルール

Iceaxeではクラス名の接頭辞として（基本的に） `Tsurugi` という単語を付けている。
これは、何も付けないとTsubkuroやその他のライブラリーとクラス名（単純名）が同一になってしまい、コーディングやソースコードリーディングの際に混乱を来す為である。

Javaでは、クラス名やメソッド名に使用する単語は省略しないことが推奨されている。
しかし多くの単語が連なると冗長な名前になってしまうので、Iceaxeでは以下のような略語を使うことがある。

| 単語               | 略語 |
| ------------------ | ---- |
| Tsurugi            | Tg   |
| Transaction        | Tx   |
| TransactionManager | Tm   |



## Tsubakuroとの対応

Iceaxeの主要なクラスは、Tsubakuroのクラスをラップ（内包）している。

| Iceaxeクラス                                           | Tsubakuroクラス                                            |
| ------------------------------------------------------ | ---------------------------------------------------------- |
| TsurugiConnector                                       | Connector                                                  |
| TsurugiSession                                         | Session・SqlClient                                         |
| TsurugiTransaction                                     | Transaction                                                |
| TgTxOption                                             | SqlRequest.TransactionType<br>SqlRequest.TransactionOption |
| TgCommitType                                           | SqlRequest.CommitStatus                                    |
| TsurugiSqlPreparedQuery<br>TsurugiSqlPreparedStatement | PreparedStatement                                          |
| TgParameterMapping                                     | SqlRequest.Placeholder<br>SqlRequest.Parameter             |
| TgBindVariable・TgBindVariables                        | SqlRequest.Placeholder                                     |
| TgBindParameter・TgBindParameters                      | SqlRequest.Parameter                                       |
| TsurugiQueryResult<br>TsurugiResultRecord              | ResultSet                                                  |
| TgDataType                                             | SqlCommon.AtomType                                         |

### トランザクション種別

Iceaxeのトランザクション種別の名称は、TsubakuroのTransactionTypeとは異なっている。

| Iceaxeトランザクション種別 | Tsubakuro TransactionType | 備考（TransactionTypeのJavadoc）                    |
| -------------------------- | ------------------------- | --------------------------------------------------- |
| OCC                        | SHORT                     | short transactions (optimistic concurrency control) |
| LTX                        | LONG                      | long transactions (pessimistic concurrency control) |
| RTX                        | READ_ONLY                 | read only transactions                              |

Tsurugiの開発初期の頃は、LTXは「長いトランザクションのうち、読み書きを伴うもの（read-write）」、RTXは「長いトランザクションのうち、読み取り専用のもの（read only）」と呼ばれていた。そのため、LTXとRTXの共通親クラスはAbstractTgTxOptionLongという名前（Longという接尾辞）になっている。

### コミットオプション

コミット時に指定するコミットオプションは、TsubakuroではCommitStatusというクラスである。
しかし「ステータス」だとコミット中の状態を表す・コミットの戻り値のように思えてしまうので、IceaxeではTgCommitTypeというクラス名にした。

Tsubakuroのcommitメソッドにはコミットオプションを引数に取らないオーバーロードがあるが、Iceaxeではコミットオプションを指定するメソッドのみとした。
TsurugiTransactionManagerを使用していればTsurugiTransactionのcommitメソッドをアプリケーション開発者が直接呼ぶことは無いので、引数の無いcommitメソッドの有無はアプリケーション開発者にとって関係ないだろう。

### TsurugiSql

SQL文やバインド変数定義を保持するクラスの共通親クラスはTsurugiSqlというクラス名だが、元々はTsubakuroのPreparedStatementクラスをラップする目的だった（TsurugiPreparedStatementというクラス名だった）為、Iceaxe内やサンプルソースコード（iceaxe-examples）でTsurugiSqlインスタンスを代入する変数の名前は `ps` になっている（`ps`のままである）。

### タイムアウトの扱い

Tsubakuroのメソッドを呼び出して返ってくるFutureResonseから値を取得する為のgetやawaitメソッドには、引数でタイムアウト時間を指定するものと指定しないもの（タイムアウトせずに無限に待つ）があるが、Iceaxeではタイムアウト時間を指定するgetメソッドのみを使用している。

ただし、Iceaxeのデフォルトのタイムアウト時間はLong.MAX_VALUEナノ秒（実質、無限）である。