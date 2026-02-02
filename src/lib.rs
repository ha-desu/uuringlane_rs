use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use sysinfo::System;
use thiserror::Error;
use uuid::Uuid;

/// メモリ行を表す構造体
///
/// `UuRingLaneRow` は、UuRingLaneエンジンが管理する最小単位のレコードです。
/// 実行時に決定される「次元数（Dimension）」に合わせて、固定長の [Uuid] スライスを保持します。
///
/// # 特徴
/// - **次元の柔軟性**: 初期化時に [UuRingLane::init] で指定された次元数に従います。
/// - **メモリ効率**: 余計なメタデータを持たず、純粋なUUIDの配列としてヒープ上に確保されます。
///
/// # 例
/// 2次元データ（投稿ID, 著者ID）の場合:
/// ```rust
/// use uuringlane::UuRingLaneRow;
/// use uuid::Uuid;
///
/// let row = UuRingLaneRow {
///     values: Box::new([Uuid::now_v7(), Uuid::now_v7()]),
/// };
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UuRingLaneRow {
    /// 次元数分のUUIDを保持するボックス化されたスライス
    pub values: Box<[Uuid]>,
}

/// UuRingLaneの操作中に発生する可能性のあるエラー
///
/// メモリ不足、不整合なデータ次元、重複登録などを通知します。
#[derive(Error, Debug)]
pub enum UuRingLaneError {
    /// メモリ割り当ての事前チェックに失敗した場合に発生します。
    ///
    /// UuRingLaneは初期化時に必要なメモリ量を計算し、システム全体の利用可能メモリの80%を超える場合に
    /// 安全のためにこのエラーを返して初期化を阻止します。
    #[error("Memory allocation check failed: requested {requested_mb}MB, but available is {available_mb}MB. (Safety limit active)")]
    InsufficientMemory {
        /// 要求されたメモリ量（MB）
        requested_mb: u64,
        /// 現在利用可能なシステムメモリ量（MB）
        available_mb: u64,
    },

    /// 指定されたラベル（名前）のデータプールが既に登録されている場合に発生します。
    ///
    /// 同じ名前で `init` を二重に呼び出すことはできません。
    #[error("Data pool '{0}' already exists in registry.")]
    AlreadyRegistered(String),

    /// データの次元数（UUIDの数）が、初期化時に設定された次元数と一致しない場合に発生します。
    #[error("Dimension mismatch: expected {expected}, but got {found}.")]
    DimensionMismatch {
        /// プールが期待する次元数
        expected: usize,
        /// 渡されたデータの実際の次元数
        found: usize,
    },
}

/// グローバルなレジストリ
static REGISTRY: Lazy<DashMap<String, UuRingLane>> = Lazy::new(DashMap::new);

/// UuRingLane
///
/// UuRingLaneは、特定のコンテキスト（例: タイムライン、トレンド、チャットルーム）に関連する
/// 大量のUUIDデータをメモリ上に保持・管理するメイン構造体です。
///
/// # 仕組み
/// - **グローバルレジストリ**: 生成されたインスタンスは内部的なグローバルマップに登録され、名前（ラベル）でどこからでもアクセス可能です。
/// - **スレッドセーフ**: 内部状態は `RwLock` で保護されており、並行アクセスに対して安全です。
/// - **FIFO（First-In-First-Out）**: 設定された `capacity` を超えるデータが挿入された場合、最も古いデータから自動的に削除されます。
#[derive(Clone)]
pub struct UuRingLane {
    inner: Arc<RwLock<UuRingLaneStore>>,
}

struct UuRingLaneStore {
    name: String,
    buffer: VecDeque<Arc<UuRingLaneRow>>,
    capacity: usize,
    dimension: usize,
}

impl UuRingLane {
    /// 指定したラベルと次元数でデータプールを初期化し、グローバルレジストリに登録します。
    ///
    /// 新しいメモリ領域を確保する前に、システムの空きメモリ量を確認する安全機構が働きます。
    ///
    /// # 引数
    /// * `label` - プールを一意に識別するための名前。後で `find` メソッドで使用します。
    /// * `capacity` - 保持する [UuRingLaneRow] の最大件数。これを超えると古いデータから破棄されます。
    /// * `dimension` - 1行あたりのUUIDの数（例: 2なら [PostID, AuthorID]）。全ての行はこの次元数を守る必要があります。
    ///
    /// # 戻り値
    /// * `Ok(UuRingLane)` - 初期化に成功したインスタンス。
    /// * `Err(UuRingLaneError::AlreadyRegistered)` - 指定したラベルが既に存在する場合。
    /// * `Err(UuRingLaneError::InsufficientMemory)` - 要求された容量が安全なメモリ制限（空き容量の80%）を超える場合。
    ///
    /// # 例
    /// ```rust
    /// use uuringlane::UuRingLane;
    ///
    /// // 最大100万件、2次元のデータプールを作成
    /// let pool = UuRingLane::init("global_timeline", 1_000_000, 2);
    /// ```
    pub fn init(label: &str, capacity: usize, dimension: usize) -> Result<Self, UuRingLaneError> {
        if REGISTRY.contains_key(label) {
            return Err(UuRingLaneError::AlreadyRegistered(label.to_string()));
        }

        let uuringlane = Self::try_new(label, capacity, dimension, true)?;
        REGISTRY.insert(label.to_string(), uuringlane.clone());
        Ok(uuringlane)
    }

    /// 名前を指定して、登録済みのインスタンスを呼び出します。
    ///
    /// アプリケーションの任意の場所から、初期化済みのデータプールへの参照を取得するために使用します。
    ///
    /// # 引数
    /// * `label` - 初期化時に指定したプールの名前。
    ///
    /// # 戻り値
    /// * `Some(UuRingLane)` - 見つかった場合、そのクローン（軽量な参照カウントポインタ）を返します。
    /// * `None` - 指定した名前のプールが存在しない場合。
    pub fn find(label: &str) -> Option<UuRingLane> {
        REGISTRY.get(label).map(|e| e.value().clone())
    }

    /// 【Private】内部的な初期化ロジック。
    fn try_new(
        name: &str,
        capacity: usize,
        dimension: usize,
        use_safety_limit: bool,
    ) -> Result<Self, UuRingLaneError> {
        if use_safety_limit {
            // Arc<UuRingLaneRow> のヒープ消費量:
            // Arc 制御ブロック(約24B) + UuRingLaneRow(16B) + ヒープ上の実体(16B * dimension)
            let row_mem =
                40 + std::mem::size_of::<UuRingLaneRow>() as u64 + (16 * dimension as u64);
            let required_bytes = capacity as u64 * row_mem;

            let mut sys = System::new_all();
            sys.refresh_memory();
            let available_bytes = sys.available_memory();
            let safety_limit = (available_bytes as f64 * 0.8) as u64;

            if safety_limit > 0 && required_bytes > safety_limit {
                return Err(UuRingLaneError::InsufficientMemory {
                    requested_mb: required_bytes / 1024 / 1024,
                    available_mb: available_bytes / 1024 / 1024,
                });
            }
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(UuRingLaneStore {
                name: name.to_string(),
                buffer: VecDeque::with_capacity(capacity),
                capacity,
                dimension,
            })),
        })
    }

    /// データのバッチ（複数据）をプールに挿入します。
    ///
    /// データはリストの先頭（最新）に追加されます。容量制限（capacity）を超えた場合、
    /// リストの末尾（最古）から自動的に削除されます。
    ///
    /// # 引数
    /// * `items` - [UuRingLaneRow] のイテレータ（`Vec` など）。
    ///
    /// # エラー
    /// * `UuRingLaneError::DimensionMismatch` - 挿入しようとしたデータの次元数が、プール設定と異なる場合。
    ///   一部のデータでも不整合があれば、挿入処理は中断されエラーが返ります。
    pub fn insert_batch<I>(&self, items: I) -> Result<(), UuRingLaneError>
    where
        I: IntoIterator<Item = UuRingLaneRow>,
    {
        let mut store = self.inner.write();
        let dim = store.dimension;

        for item in items {
            if item.values.len() != dim {
                return Err(UuRingLaneError::DimensionMismatch {
                    expected: dim,
                    found: item.values.len(),
                });
            }

            if store.buffer.len() >= store.capacity {
                store.buffer.pop_back();
            }
            store.buffer.push_front(Arc::new(item));
        }
        Ok(())
    }

    /// 指定したインデックス（カラム）に基づいてデータを高速検索（キュレーション）し、抽出します。
    ///
    /// メモリ上の全データを走査し、条件に一致する行を抽出して返します。
    /// 結果は元のデータ構造（[UuRingLaneRow]）のリストとして返されます。
    ///
    /// # 引数
    /// * `index` - 判定対象とするカラムのインデックス（0始まり）。
    /// * `targets` - 検索対象となるIDのセット（HashSet）。これに含まれるIDを持つ行がヒットします。
    /// * `limit` - 抽出する最大件数。この数に達した時点で検索を打ち切ります。
    ///
    /// # 戻り値
    /// 条件に一致した [UuRingLaneRow] の参照（[Arc]）のベクタ。
    ///
    /// # 例
    /// インデックス1（著者ID）が自分のフォローリストに含まれる投稿を検索する場合：
    /// ```ignore
    /// let results = pool.curate_by(1, &following_user_ids, 20);
    /// ```
    pub fn curate_by(
        &self,
        index: usize,
        targets: &HashSet<Uuid>,
        limit: usize,
    ) -> Vec<Arc<UuRingLaneRow>> {
        let store = self.inner.read();
        store
            .buffer
            .iter()
            .filter(|s| {
                // いずれかのカラムに Uuid::nil() が含まれる場合は、論理的に削除（忘却）されたとみなしてスキップ
                if s.values.iter().any(|v| v.is_nil()) {
                    return false;
                }
                s.values
                    .get(index)
                    .map_or(false, |val| targets.contains(val))
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// 指定された次元（インデックス）の値が ID と一致する行を特定し、そのスロットを Uuid::nil() で上書きします。
    ///
    /// これにより、その行は以降の `curate_by` などの検索から論理的に除外（忘却）されます。
    ///
    /// # 引数
    /// * `index` - 検索対象とする次元のインデックス。
    /// * `id` - 忘却させたい UUID。
    pub fn purge_by_id(&self, index: usize, id: Uuid) {
        if id.is_nil() {
            return;
        }
        let mut store = self.inner.write();
        for row in store.buffer.iter_mut() {
            if let Some(val) = row.values.get(index) {
                if *val == id {
                    // 検索等で共有されている可能性があるため、クローンしてから置換（Copy-on-Write）
                    let mut new_row = (**row).clone();
                    if let Some(v) = new_row.values.get_mut(index) {
                        *v = Uuid::nil();
                    }
                    *row = Arc::new(new_row);
                }
            }
        }
    }

    /// UUID v7 の時系列特性を利用して、指定されたミリ秒（Unix Epoch）より古いデータを論理的に削除（忘却）します。
    ///
    /// 内部的には `as_u128()` による高速なビット比較を行い、閾値未満の UUID を持つスロットを `Uuid::nil()` で上書きします。
    /// 追加のメモリ（タイムスタンプ保持用など）は一切使用せず、フラットなデータ構造を維持します。
    ///
    /// # 引数
    /// * `index` - 比較対象の UUID v7 が格納されている次元のインデックス。
    /// * `threshold_ms` - 削除の閾値となる Unix タイムスタンプ（ミリ秒）。これより古いデータが削除されます。
    pub fn vacate_expired(&self, index: usize, threshold_ms: u64) {
        // UUID v7 の上位48ビットはミリ秒単位のタイムスタンプ
        let threshold_u128 = (threshold_ms as u128) << 80;
        let mut store = self.inner.write();
        for row in store.buffer.iter_mut() {
            if let Some(val) = row.values.get(index) {
                // nil でなく、かつタイムスタンプが閾値未満の場合は忘却させる
                if !val.is_nil() && val.as_u128() < threshold_u128 {
                    let mut new_row = (**row).clone();
                    if let Some(v) = new_row.values.get_mut(index) {
                        *v = Uuid::nil();
                    }
                    *row = Arc::new(new_row);
                }
            }
        }
    }

    /// 既存のデータをすべて破棄し、指定された新しいデータセットでメモリを満たします。
    ///
    /// サーバー再起動時や、DBからのロード時にキャッシュを暖気（Warm-up）するために使用します。
    /// 指定されたデータが容量（capacity）を超える場合、超過分は無視（切り捨て）されます。
    ///
    /// # エラー
    /// * `UuRingLaneError::DimensionMismatch` - データの次元数がプール設定と一致しない場合。
    pub fn restore(&self, rows: Vec<UuRingLaneRow>) -> Result<(), UuRingLaneError> {
        let mut store = self.inner.write();
        let dim = store.dimension;
        let limit = store.capacity;

        // 次元のチェックを追加
        for (i, row) in rows.iter().enumerate() {
            if row.values.len() != dim {
                return Err(UuRingLaneError::DimensionMismatch {
                    expected: dim,
                    found: row.values.len(),
                });
            }
            if i >= limit {
                break;
            }
        }

        store.buffer.clear();
        store
            .buffer
            .extend(rows.into_iter().take(limit).map(Arc::new));
        Ok(())
    }

    /// データプールを完全に破棄し、メモリをOSに返却します。
    ///
    /// 内部バッファをクリアして `shrink_to_fit` を呼び出した後、
    /// グローバルレジストリからこのプールのエントリーを削除します。
    ///
    /// 一度 `vacate` された名前のプールは、再度 `init` できるようになります。
    pub fn vacate(&self) {
        let name = {
            let mut store = self.inner.write();
            store.buffer.clear();
            store.buffer.shrink_to_fit();
            store.name.clone()
        };

        REGISTRY.remove(&name);
    }

    /// プールの名前（ラベル）を取得します。
    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }
    /// 現在保持しているデータ件数を取得します。
    pub fn len(&self) -> usize {
        self.inner.read().buffer.len()
    }
    /// データプールが空かどうかを判定します。
    pub fn is_empty(&self) -> bool {
        self.inner.read().buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_find() {
        let label = "test_pool";
        // 初期化できるか
        let result = UuRingLane::init(label, 100, 2);
        assert!(result.is_ok());

        // findで呼び出せるか
        let pool = UuRingLane::find(label);
        assert!(pool.is_some());
        assert_eq!(pool.unwrap().name(), label);

        // 後片付け（他のテストに影響しないように）
        UuRingLane::find(label).unwrap().vacate();
    }

    #[test]
    fn test_insert_and_curate() {
        let label = "curate_pool";
        let pool = UuRingLane::init(label, 10, 2).unwrap();

        let post_id = Uuid::now_v7();
        let author_id = Uuid::now_v7();

        // データの挿入
        let row = UuRingLaneRow {
            values: Box::new([post_id, author_id]),
        };
        pool.insert_batch(vec![row]).unwrap();

        // 正しく探査できるか（author_idで検索してpost_idが返るか）
        let mut targets = HashSet::new();
        targets.insert(author_id);

        let results = pool.curate_by(1, &targets, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], post_id);

        pool.vacate();
    }

    #[test]
    fn test_purge_by_id() {
        let label = "purge_pool";
        let pool = UuRingLane::init(label, 10, 2).unwrap();

        let post_id = Uuid::now_v7();
        let author_id = Uuid::now_v7();

        pool.insert_batch(vec![UuRingLaneRow {
            values: Box::new([post_id, author_id]),
        }])
        .unwrap();

        // 1. 最初は検索にヒットする
        let mut targets = HashSet::new();
        targets.insert(author_id);
        assert_eq!(pool.curate_by(1, &targets, 10).len(), 1);

        // 2. purge_by_id で PostID を指定して「忘却」させる
        pool.purge_by_id(0, post_id);

        // 3. AuthorID で検索してもヒットしなくなる（行内に nil が含まれるため）
        assert_eq!(pool.curate_by(1, &targets, 10).len(), 0);

        pool.vacate();
    }

    #[test]
    fn test_vacate_expired() {
        let label = "expire_pool";
        let pool = UuRingLane::init(label, 10, 1).unwrap();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // 1. 10秒前のUUIDを作成して挿入
        // (厳密には version 7 ではないが、u128比較のロジックテストとして十分)
        let old_ts = now_ms - 10000;
        let old_uuid = Uuid::from_u128((old_ts as u128) << 80);
        pool.insert_batch(vec![UuRingLaneRow {
            values: Box::new([old_uuid]),
        }])
        .unwrap();

        // 2. 現在のUUIDを挿入
        let new_uuid = Uuid::now_v7();
        pool.insert_batch(vec![UuRingLaneRow {
            values: Box::new([new_uuid]),
        }])
        .unwrap();

        assert_eq!(pool.len(), 2);

        // 3. 5秒前を閾値にして期限切れを削除
        pool.vacate_expired(0, now_ms - 5000);

        // 4. curate_by で確認（old_uuid は消え、new_uuid だけ残っているはず）
        let mut targets = HashSet::new();
        targets.insert(old_uuid);
        targets.insert(new_uuid);

        let results = pool.curate_by(0, &targets, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], new_uuid);

        pool.vacate();
    }

    #[test]
    fn test_dimension_mismatch() {
        let label = "dim_pool";
        let pool = UuRingLane::init(label, 10, 2).unwrap();

        // 2次元のはずなのに1次元を入れようとするとエラーになるか
        let bad_row = UuRingLaneRow {
            values: Box::new([Uuid::now_v7()]),
        };
        let result = pool.insert_batch(vec![bad_row]);
        assert!(result.is_err());

        pool.vacate();
    }
}

#[test]
fn test_capacity_rotation() {
    let label = "rotation_pool";
    // 容量3のプールを作成
    let pool = UuRingLane::init(label, 3, 1).unwrap();

    let id1 = Uuid::now_v7();
    let id2 = Uuid::now_v7();
    let id3 = Uuid::now_v7();
    let id4 = Uuid::now_v7();

    // 4件挿入（id1 -> id2 -> id3 -> id4 の順）
    // バッファは push_front なので、新しい順に並ぶ
    pool.insert_batch(vec![UuRingLaneRow {
        values: Box::new([id1]),
    }])
    .unwrap();
    pool.insert_batch(vec![UuRingLaneRow {
        values: Box::new([id2]),
    }])
    .unwrap();
    pool.insert_batch(vec![UuRingLaneRow {
        values: Box::new([id3]),
    }])
    .unwrap();
    pool.insert_batch(vec![UuRingLaneRow {
        values: Box::new([id4]),
    }])
    .unwrap();

    // 容量は3なので、id4, id3, id2 が残り、一番古い id1 は消えているはず
    assert_eq!(pool.len(), 3);

    // id1 が消えていることを確認（検索してヒットしない）
    let mut targets = HashSet::new();
    targets.insert(id1);
    let res = pool.curate_by(0, &targets, 10);
    assert!(res.is_empty());

    // id4 が残っていることを確認
    targets.clear();
    targets.insert(id4);
    let res = pool.curate_by(0, &targets, 10);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].values[0], id4);

    pool.vacate();
}

#[test]
fn test_restore_overflow() {
    let label = "restore_pool";
    let pool = UuRingLane::init(label, 2, 1).unwrap();

    let rows = vec![
        UuRingLaneRow {
            values: Box::new([Uuid::now_v7()]),
        },
        UuRingLaneRow {
            values: Box::new([Uuid::now_v7()]),
        },
        UuRingLaneRow {
            values: Box::new([Uuid::now_v7()]),
        }, // 3つ目（容量オーバー）
    ];

    // restore実行
    pool.restore(rows).unwrap();

    // 容量制限(2)が守られているか
    assert_eq!(pool.len(), 2);

    pool.vacate();
}

#[test]
fn test_concurrency() {
    use std::thread;

    let label = "concurrent_pool";
    let pool = UuRingLane::init(label, 1000, 1).unwrap();
    let pool_clone_write = pool.clone();
    let pool_clone_read = pool.clone();

    // 書き込みスレッド
    let t1 = thread::spawn(move || {
        for _ in 0..100 {
            let row = UuRingLaneRow {
                values: Box::new([Uuid::now_v7()]),
            };
            pool_clone_write.insert_batch(vec![row]).unwrap();
        }
    });

    // 読み込みスレッド
    let t2 = thread::spawn(move || {
        for _ in 0..100 {
            let targets = HashSet::new(); // 空検索
            let _ = pool_clone_read.curate_by(0, &targets, 10);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // 競合せずに完了し、データが100件入っていること
    assert_eq!(pool.len(), 100);

    pool.vacate();
}
