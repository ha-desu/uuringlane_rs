use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use sysinfo::System;
use thiserror::Error;
use uuid::Uuid;

// --- Statics ---

/// システム全体で共有される `UuRingLane` インスタンスのレジストリ。
static REGISTRY: Lazy<DashMap<String, UuRingLane>> = Lazy::new(DashMap::new);

/// 並列処理の最適化に使用される L3 キャッシュサイズ。
/// L3 が取得できない場合は L2、それでも不明な場合は 16MB をデフォルトとします。
static L3_CACHE_SIZE: Lazy<usize> = Lazy::new(|| {
    cache_size::l3_cache_size()
        .or_else(cache_size::l2_cache_size)
        .unwrap_or(16 * 1024 * 1024)
});

// --- Errors ---

/// `UuRingLane` の操作中に発生する可能性のあるエラーを定義します。
#[derive(Error, Debug)]
pub enum UuRingLaneError {
    /// メモリの空き容量が不足している場合に発生します。
    #[error("Memory allocation check failed: requested {requested_mb}MB, but available is {available_mb}MB.")]
    InsufficientMemory {
        requested_mb: u64,
        available_mb: u64,
    },

    /// 同一名のプールが既にレジストリに登録されている場合に発生します。
    #[error("Data pool '{0}' already exists in registry.")]
    AlreadyRegistered(String),

    /// 指定されたデータとプールの次元数が一致しない場合に発生します。
    #[error("Dimension mismatch in pool '{pool_name}': expected {expected}, but got {found}.")]
    DimensionMismatch {
        pool_name: String,
        expected: usize,
        found: usize,
    },

    /// プールの内部状態が矛盾している場合に発生します。
    #[error("Internal inconsistency in pool '{name}': {message}")]
    InconsistentState { name: String, message: String },

    /// 指定された名前のプールが見つからない場合に発生します。
    #[error("Pool '{0}' not found or already vacated.")]
    PoolNotFound(String),

    /// 値が UUID v7/v8 フォーマットに従っていない場合に発生します。
    #[error("Invalid UuField format: All IDs must be UUID v8 with 48-bit timestamp.")]
    InvalidEvvIdFormat,
}

// --- Core Data Types ---

/// インデックスの動作モードを定義します。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LaneMode {
    /// 指定された次元数で固定されたモード。
    Fixed(usize),
    /// 次元数を動的に扱える柔軟なモード。
    Flexible,
}

/// `UuField` は `UuRingLane` における最小のデータ単位です。
///
/// 128ビットのうち、上位48ビットをタイムスタンプ、4ビットをバージョン、
/// 2ビットをバリアント、残りの74ビットを自由なペイロードとして管理します。
/// UUID v7/v8 フォーマットと互換性があり、時系列順のソートが可能です。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct UuField(u128);

impl UuField {
    const VERSION_MASK: u128 = 0x0000_0000_0000_F000_0000_0000_0000_0000;
    const VARIANT_MASK: u128 = 0x0000_0000_0000_0000_C000_0000_0000_0000;
    const PAYLOAD_MASK: u128 = 0x0000_0000_0000_0000_3FFF_FFFF_FFFF_FFFF;

    /// 与えられた `u128` 値が UUID v7 または v8 フォーマットとして有効かどうかを判定します。
    pub fn is_valid(val: u128) -> bool {
        let version = (val & Self::VERSION_MASK) >> 76;
        let variant = (val & Self::VARIANT_MASK) >> 62;

        // v7 (111) または v8 (1000) かつ RFC4122 (10xx)
        (version == 7 || version == 8) && variant == 2
    }

    /// 指定されたタイムスタンプ（ミリ秒）とペイロード（最大74ビット）から新しい `UuField` を生成します。
    pub fn new(timestamp_ms: u64, payload: u128) -> Self {
        let ts = (timestamp_ms as u128) << 80;
        let ver = 8u128 << 76;
        let var = 2u128 << 62;
        let p = payload & Self::PAYLOAD_MASK;

        Self(ts | ver | var | p)
    }

    /// 生の `u128` から `UuField` を生成します。バリデーションは行われません。
    pub const fn from_u128_unchecked(val: u128) -> Self {
        Self(val)
    }

    /// 生の `u128` からバリデーション付きで `UuField` への変換を試みます。
    pub fn try_from_u128(val: u128) -> Option<Self> {
        if Self::is_valid(val) {
            Some(Self(val))
        } else {
            None
        }
    }

    /// 内部の `u128` 値を返します。
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    /// 上位48ビットのタイムスタンプ（ミリ秒）を抽出します。
    pub fn timestamp(&self) -> u64 {
        (self.0 >> 80) as u64
    }

    /// 下位74ビットのペイロード値を抽出します。
    pub fn payload(&self) -> u128 {
        self.0 & Self::PAYLOAD_MASK
    }
}

impl From<UuField> for Uuid {
    fn from(field: UuField) -> Self {
        Uuid::from_u128(field.as_u128())
    }
}

// --- Row Structures ---

/// 固定された次元数 `N` を持つ、キャッシュアラインメント済みの行データ構造体です。
/// パフォーマンスが最優先される場合に適しています。
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct UuRingLaneRow<const N: usize> {
    pub values: [u128; N],
}

impl<const N: usize> UuRingLaneRow<N> {
    /// 新しい固定次元の行データを生成します。
    pub fn new(values: [u128; N]) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<[Uuid; N]> for UuRingLaneRow<N> {
    fn from(uuids: [Uuid; N]) -> Self {
        let values = uuids.map(|u| u.as_u128());
        Self { values }
    }
}

impl<const N: usize> From<UuRingLaneRow<N>> for [Uuid; N] {
    fn from(row: UuRingLaneRow<N>) -> Self {
        row.values.map(Uuid::from_u128)
    }
}

/// 動的な次元数を持つ行データ構造体です。次元数が不特定、または頻繁に変更される場合に適しています。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UuRingLaneFlexibleRow {
    pub values: Box<[u128]>,
}

impl UuRingLaneFlexibleRow {
    /// 新しい動的な行データを生成します。
    pub fn new(values: Vec<u128>) -> Self {
        Self {
            values: values.into_boxed_slice(),
        }
    }
}

impl From<UuRingLaneFlexibleRow> for Vec<Uuid> {
    fn from(row: UuRingLaneFlexibleRow) -> Self {
        row.values.iter().map(|&v| Uuid::from_u128(v)).collect()
    }
}

impl From<Vec<Uuid>> for UuRingLaneFlexibleRow {
    fn from(uuids: Vec<Uuid>) -> Self {
        Self {
            values: uuids
                .into_iter()
                .map(|u| u.as_u128())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

// --- Core Engine structures ---

/// `UuRingLane` は、メモリ上での高速な時系列検索に特化したリングバッファ型のインデックスエンジンです。
///
/// 複数のカラム（次元）を持つデータを一定容量まで保持し、UUID v7 の特性を活かした
/// 高速なフィルタリングと集計機能を提供します。
#[derive(Clone)]
pub struct UuRingLane {
    inner: Arc<RwLock<UuRingLaneStore>>,
}

/// 以前のプロジェクト名との互換性のためのエイリアス。
pub use UuRingLane as Newsy;
/// 2次元データの固定長行。
pub type NewsyRow2 = UuRingLaneRow<2>;

/// 内部の状態を保持する構造体。
struct UuRingLaneStore {
    name: String,
    /// 連続したメモリ空間にフラットに展開されたデータバッファ。
    buffer: Vec<u128>,
    /// 次に書き込む行のインデックス。
    head: usize,
    /// 現在プールに格納されている有効な行数。
    len: usize,
    /// 最大収容可能行数。
    capacity: usize,
    /// 1行あたりのデータ項目数。
    dimension: usize,
    /// 動作モード。
    mode: LaneMode,
}

// --- Implementation ---

impl UuRingLane {
    /// 新しい `UuRingLane` インスタンスを `Flexible` モードで初期化し、グローバルレジストリに登録します。
    pub fn new_flexible(
        label: &str,
        capacity: usize,
        dimension: usize,
    ) -> Result<Self, UuRingLaneError> {
        if REGISTRY.contains_key(label) {
            return Err(UuRingLaneError::AlreadyRegistered(label.to_string()));
        }

        let uuringlane = Self::try_new(label, capacity, dimension, LaneMode::Flexible, true)?;
        REGISTRY.insert(label.to_string(), uuringlane.clone());
        Ok(uuringlane)
    }

    /// 新しい `UuRingLane` インスタンスを `Fixed` モードで初期化し、グローバルレジストリに登録します。
    pub fn new_fixed(
        label: &str,
        capacity: usize,
        dimension: usize,
    ) -> Result<Self, UuRingLaneError> {
        if REGISTRY.contains_key(label) {
            return Err(UuRingLaneError::AlreadyRegistered(label.to_string()));
        }

        let uuringlane =
            Self::try_new(label, capacity, dimension, LaneMode::Fixed(dimension), true)?;
        REGISTRY.insert(label.to_string(), uuringlane.clone());
        Ok(uuringlane)
    }

    /// レジストリから指定された名前のインスタンスを検索します。
    pub fn find(label: &str) -> Option<UuRingLane> {
        REGISTRY.get(label).map(|e| e.value().clone())
    }

    /// 内部的な初期化処理。メモリチェックを行い、バッファを確保します。
    fn try_new(
        name: &str,
        capacity: usize,
        dimension: usize,
        mode: LaneMode,
        use_safety_limit: bool,
    ) -> Result<Self, UuRingLaneError> {
        if use_safety_limit {
            let row_mem = 16 * dimension as u64;
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
                buffer: vec![0u128; capacity * dimension],
                head: 0,
                len: 0,
                capacity,
                dimension,
                mode,
            })),
        })
    }

    /// データのバッチをリングバッファに追加します。
    /// 容量を超えると、古いデータから上書きされます。
    pub fn insert_batch<I, T>(&self, items: I) -> Result<(), UuRingLaneError>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u128]>,
    {
        let mut store = self.inner.write();
        let dim = store.dimension;
        let cap = store.capacity;

        for item in items {
            let slice = item.as_ref();
            Self::validate_row(slice)?;
            if slice.len() != dim {
                return Err(UuRingLaneError::DimensionMismatch {
                    pool_name: store.name.clone(),
                    expected: dim,
                    found: slice.len(),
                });
            }

            let start = store.head * dim;
            store.buffer[start..start + dim].copy_from_slice(slice);

            store.head = (store.head + 1) % cap;
            if store.len < cap {
                store.len += 1;
            }
        }
        Ok(())
    }

    /// 固定長配列を用いた高速なバッチ挿入を提供します。
    pub fn insert_fixed<const N: usize>(
        &self,
        items: Vec<UuRingLaneRow<N>>,
    ) -> Result<(), UuRingLaneError> {
        let mut store = self.inner.write();
        let dim = store.dimension;
        let cap = store.capacity;

        if N != dim {
            return Err(UuRingLaneError::DimensionMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: N,
            });
        }

        for item in items {
            Self::validate_row(&item.values)?;
            let start = store.head * dim;
            store.buffer[start..start + dim].copy_from_slice(&item.values);

            store.head = (store.head + 1) % cap;
            if store.len < cap {
                store.len += 1;
            }
        }
        Ok(())
    }

    /// 指定されたインデックス（次元）の値を検索キーとして、並列フィルタリングを実行します。
    /// 結果は時系列（挿入）の逆順で返されます。
    pub fn curate_by(
        &self,
        index: usize,
        targets: &HashSet<Uuid>,
        limit: usize,
    ) -> Vec<Arc<[u128]>> {
        if targets.is_empty() {
            return Vec::new();
        }

        let target_u128s: HashSet<u128> = targets.iter().map(|u| u.as_u128()).collect();
        let store = self.inner.read();
        let dim = store.dimension;
        let len = store.len;
        let head = store.head;

        let min_len = Self::calculate_min_len(dim);

        let (front, back) = store.buffer[..len * dim].split_at(head * dim);

        let mut results: Vec<Arc<[u128]>> = front
            .par_chunks_exact(dim)
            .rev()
            .chain(back.par_chunks_exact(dim).rev())
            .with_min_len(min_len)
            .filter(|row| {
                if row.iter().any(|&v| v == 0) {
                    return false;
                }
                row.get(index)
                    .map_or(false, |val| target_u128s.contains(val))
            })
            .map(Arc::from)
            .collect();

        results.truncate(limit);
        results
    }

    /// 特定のIDに一致するデータをプールから削除（ゼロでマスク）します。
    pub fn purge_by_id(&self, index: usize, id: Uuid) {
        let id_u128 = id.as_u128();
        if id_u128 == 0 {
            return;
        }
        let mut store = self.inner.write();
        let dim = store.dimension;
        let len = store.len;

        let min_len = Self::calculate_min_len(dim);

        store.buffer[..len * dim]
            .par_chunks_exact_mut(dim)
            .with_min_len(min_len)
            .for_each(|row| {
                if let Some(&val) = row.get(index) {
                    if val == id_u128 {
                        row.fill(0);
                    }
                }
            });
    }

    /// 指定されたしきい値（ミリ秒）よりも古いデータを、UUID v7 の特性を利用して削除（ゼロでマスク）します。
    pub fn vacate_expired(&self, index: usize, threshold_ms: u64) {
        let threshold_u128 = (threshold_ms as u128) << 80;
        let mut store = self.inner.write();
        let dim = store.dimension;
        let len = store.len;

        let min_len = Self::calculate_min_len(dim);

        store.buffer[..len * dim]
            .par_chunks_exact_mut(dim)
            .with_min_len(min_len)
            .for_each(|row| {
                if let Some(&val) = row.get(index) {
                    if val != 0 && val < threshold_u128 {
                        row.fill(0);
                    }
                }
            });
    }

    /// プールの状態をリセットし、新しいデータセットで満たします。初期データのロードやテストに使用します。
    pub fn restore<T>(&self, rows: Vec<T>) -> Result<(), UuRingLaneError>
    where
        T: AsRef<[u128]>,
    {
        let mut store = self.inner.write();
        let dim = store.dimension;
        let cap = store.capacity;
        let input_len = rows.len().min(cap);

        store.buffer.fill(0);

        for (i, row) in rows.iter().take(input_len).enumerate() {
            let slice = row.as_ref();
            if slice.len() != dim {
                return Err(UuRingLaneError::DimensionMismatch {
                    pool_name: store.name.clone(),
                    expected: dim,
                    found: slice.len(),
                });
            }

            Self::validate_row(slice)?;

            let start = i * dim;
            store.buffer[start..start + dim].copy_from_slice(slice);
        }

        store.len = input_len;
        store.head = input_len % cap;

        Ok(())
    }

    /// インスタンスを破棄し、メモリをOSに返却します。
    pub fn vacate(&self) {
        let name = {
            let mut store = self.inner.write();
            store.buffer.clear();
            store.buffer.shrink_to_fit();
            store.name.clone()
        };
        REGISTRY.remove(&name);
    }

    /// プールの動作モードを取得します。
    pub fn mode(&self) -> LaneMode {
        self.inner.read().mode
    }
    /// プールの名前（ラベル）を取得します。
    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }
    /// 現在の有効な行数を取得します。
    pub fn len(&self) -> usize {
        self.inner.read().len
    }
    /// プールが空かどうかを判定します。
    pub fn is_empty(&self) -> bool {
        self.inner.read().len == 0
    }

    /// L3 キャッシュサイズに基づいて、最適な並列分割単位（min_len）を計算します。
    fn calculate_min_len(dim: usize) -> usize {
        let cpus = rayon::current_num_threads();
        let l3_cache_size = *L3_CACHE_SIZE;
        let row_size = 16 * dim;
        let max_rows_in_cache = l3_cache_size / row_size;

        (max_rows_in_cache / cpus).clamp(500, 5000)
    }

    /// 行データ全体が `UuField` 規約を遵守しているかチェックします。
    fn validate_row(row: &[u128]) -> Result<(), UuRingLaneError> {
        for &val in row {
            if val != 0 && !UuField::is_valid(val) {
                return Err(UuRingLaneError::InvalidEvvIdFormat);
            }
        }
        Ok(())
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_find() {
        let label = "test_pool";
        let _ = UuRingLane::new_fixed(label, 100, 2).unwrap();
        let pool = UuRingLane::find(label);
        assert!(pool.is_some());
        assert_eq!(pool.unwrap().name(), label);
        UuRingLane::find(label).unwrap().vacate();
    }

    #[test]
    fn test_conversion_fixed() {
        let u1 = Uuid::now_v7();
        let u2 = Uuid::now_v7();
        let arr = [u1, u2];
        let row = UuRingLaneRow::from(arr);
        assert_eq!(row.values[0], u1.as_u128());
        let arr2: [Uuid; 2] = row.into();
        assert_eq!(arr, arr2);
    }

    #[test]
    fn test_alignment() {
        assert_eq!(std::mem::align_of::<UuRingLaneRow<2>>(), 64);
    }
    #[test]
    fn test_new_flexible() {
        let label = "test_flexible";
        let lane = UuRingLane::new_flexible(label, 10, 3).unwrap();
        assert_eq!(lane.mode(), LaneMode::Flexible);

        let f1 = UuField::new(1000, 1).as_u128();
        let f2 = UuField::new(1001, 2).as_u128();
        let f3 = UuField::new(1002, 3).as_u128();
        let f4 = UuField::new(1003, 4).as_u128();
        let f5 = UuField::new(1004, 5).as_u128();
        let f6 = UuField::new(1005, 6).as_u128();

        let data = vec![[f1, f2, f3], [f4, f5, f6]];
        lane.insert_batch(&data).unwrap();
        assert_eq!(lane.len(), 2);

        let target = HashSet::from([Uuid::from_u128(f2), Uuid::from_u128(f5)]);
        let res = lane.curate_by(1, &target, 10);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0][1], f5);
        assert_eq!(res[1][1], f2);

        lane.vacate();
    }

    #[test]
    fn test_dimension_mismatch() {
        let label = "test_mismatch";
        let lane = UuRingLane::new_fixed(label, 10, 2).unwrap();

        let bad_data = vec![[1]];
        let res = lane.insert_batch(&bad_data);
        assert!(res.is_err());

        lane.vacate();
    }

    #[test]
    fn test_ring_buffer_logic() {
        let label = "test_ring";
        let f10 = UuField::new(2000, 10).as_u128();
        let f20 = UuField::new(2001, 20).as_u128();
        let f30 = UuField::new(2002, 30).as_u128();
        let f40 = UuField::new(2003, 40).as_u128();

        let lane = UuRingLane::new_fixed(label, 3, 1).unwrap();
        lane.insert_batch(&[[f10], [f20], [f30]]).unwrap();
        assert_eq!(lane.len(), 3);

        lane.insert_batch(&[[f40]]).unwrap();
        assert_eq!(lane.len(), 3);

        let targets = HashSet::from([
            Uuid::from_u128(f40),
            Uuid::from_u128(f30),
            Uuid::from_u128(f20),
        ]);
        let res = lane.curate_by(0, &targets, 10);
        assert_eq!(res.len(), 3);
        assert_eq!(res[0][0], f40);
        assert_eq!(res[1][0], f30);
        assert_eq!(res[2][0], f20);

        lane.vacate();
    }
}
