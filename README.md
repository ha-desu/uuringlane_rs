# UuRingLane

UuRingLane は、インメモリでデータを保持し、時系列に基づいた検索やフィルタリングを行うための Rust 製ライブラリです。

## 特徴

- **UuField によるデータ構造**: 128ビット（`u128`）を基本単位とし、タイムスタンプを内包する UUID v7/v8 フォーマットと互換性を持たせています。
    - 48ビット：タイムスタンプ
    - 74ビット：ペイロード
- **並列処理の利用**: `Rayon` を用いた並列走査が可能です。また、キャッシュサイズに応じた処理単位の調整を行います。
- **2つの動作モード**:
    - **Fixed モード**: キャッシュラインを意識した固定次元の構造です。
    - **Flexible モード**: インデックスの次元数を柔軟に扱える構造です。
- **メモリ制限機能**: 初期化時にシステムメモリをチェックし、指定された容量の確保が困難な場合にエラーを返す機能を備えています。
- **レジストリ機能**: インスタンスに名前を付けて管理し、必要に応じて呼び出すことができます。

## 導入

`Cargo.toml` へ以下のように記述してください：

```toml
[dependencies]
uuringlane = { git = "https://github.com/ha-desu/uuringlane_rs.git" }
uuid = { version = "1.10", features = ["v7"] }
```

## 基本的な例

```rust
use uuringlane::{UuRingLane, UuField};
use std::collections::HashSet;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 初期化
    // ラベル "main_timeline"、最大100万件、2次元で作成
    let lane = UuRingLane::new_fixed("main_timeline", 1_000_000, 2)?;

    // 2. データの追加
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let user_id = Uuid::now_v7();
    let post_id = UuField::new(now, 12345);

    // [PostID, AuthorID] を追加
    lane.insert_batch(vec![
        [post_id.as_u128(), user_id.as_u128()]
    ])?;

    // 3. 検索
    let mut targets = HashSet::new();
    targets.insert(user_id);

    // インデックス 1 (AuthorID) を対象に最大50件取得
    let results = lane.curate_by(1, &targets, 50);

    for row in results {
        let pid = Uuid::from_u128(row[0]);
        println!("Found post: {}", pid);
    }

    // 4. 解放
    lane.vacate();

    Ok(())
}
```

## その他

- **`vacate_expired`**: 指定した時間より古いデータの走査・無効化。
- **`restore`**: データのインポート。
- **`purge_by_id`**: 指定したIDに一致するデータの無効化。

## テスト

```bash
cargo test
cargo bench
```

## ライセンス

MIT License
Created by ha-desu