# external-sorter-poc

Work in progress

外部ソート用ライブラリを作る (主にはnusamai用) 

### 求めるもの

- (Key, Value) なデータを Key でソートすることに特化する
    - キーの部分だけをデコード、エンコードする（マージのたびにアイテム全体をシリアライズ・デシリアライズしない）
- ソートをキャンセルできること
- メモリ使用量を制限できること
- マルチスレッディング（当然）
- (非同期IO?)
- シンプルなAPI

### 先行例

- kv-par-merge-sort-rs: https://github.com/bonsairobo/kv-par-merge-sort-rs
    - key-value
    - tempfile, bytemuck, crossbeam-channel
- ext-sort: https://github.com/dapper91/ext-sort-rs
    - tempfile, rayon, serde, deepsize
- extsort: https://github.com/appaquet/extsort-rs
    - tempfile, rayon

### 処理の流れ

- Pass 1:
    - Stage 1:
        - ストリーミング入力を受ける
        - メモリ上でソートできる範囲で小さなチャンクに分けつつ、ファイルに書き出す
        - (バッファ長以上のデータを扱う手法もある)

    - Stage 2:
        - ファイルを K-way マージする
            - 二分ヒープ？
        - 十分なチャンク数にまでマージされたらマージを終える

- Pass 2:
    - 最後のマージをしながら要素をイテレータとして取り出す
