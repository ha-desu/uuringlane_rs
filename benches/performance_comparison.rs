use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::HashSet;
use uuid::Uuid;
use uuringlane::{UuField, UuRingLane};

fn generate_dummy_data(user_count: usize, posts_per_user: usize) -> (Vec<Uuid>, Vec<[u128; 2]>) {
    let mut users = Vec::with_capacity(user_count);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    for i in 0..user_count {
        users.push(Uuid::from_u128(UuField::new(now, i as u128).as_u128()));
    }

    let mut data = Vec::with_capacity(user_count * posts_per_user);
    let mut post_counter = 0u128;
    for &user_id in &users {
        let user_u128 = user_id.as_u128();
        for _ in 0..posts_per_user {
            let post_id = UuField::new(now, post_counter).as_u128();
            data.push([post_id, user_u128]);
            post_counter += 1;
        }
    }
    (users, data)
}

fn bench_comparison(c: &mut Criterion) {
    let user_count = 10_000;
    let posts_per_user = 500;
    let following_count = 200;
    let limit = 50;

    let (users, data) = generate_dummy_data(user_count, posts_per_user);

    // Setup HashSet implementation
    let following_users: HashSet<Uuid> = users.iter().take(following_count).cloned().collect();
    let following_u128s: HashSet<u128> = following_users.iter().map(|u| u.as_u128()).collect();

    // Setup UuRingLane
    let lane =
        UuRingLane::new_flexible("bench_comparison", user_count * posts_per_user, 2).unwrap();
    lane.insert_batch(&data).unwrap();

    let mut group = c.benchmark_group("Search Comparison (5M records)");

    group.bench_function("Native HashSet Iteration", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for item in data.iter().rev() {
                if following_u128s.contains(&item[1]) {
                    results.push(*item);
                    if results.len() >= limit {
                        break;
                    }
                }
            }
            black_box(results);
        })
    });

    group.bench_function("UuRingLane curate_by", |b| {
        b.iter(|| {
            let results = lane.curate_by(1, &following_users, limit);
            black_box(results);
        })
    });

    group.finish();
    lane.vacate();
}

fn bench_scenario(c: &mut Criterion) {
    let user_count = 1000;
    let posts_per_user = 100;
    let (users, data) = generate_dummy_data(user_count, posts_per_user);
    let following_users: HashSet<Uuid> = users.iter().take(200).cloned().collect();

    c.bench_function("Flexible Scenario: New -> Insert -> Curate", |b| {
        b.iter(|| {
            let lane =
                UuRingLane::new_flexible("bench_scenario", user_count * posts_per_user, 2).unwrap();
            lane.insert_batch(&data).unwrap();
            let results = lane.curate_by(1, &following_users, 50);
            black_box(results);
            lane.vacate();
        })
    });
}

fn bench_alignment(c: &mut Criterion) {
    let size = 100_000;
    let (users, data) = generate_dummy_data(1000, 100);
    let following_users: HashSet<Uuid> = users.iter().take(100).cloned().collect();

    // Setup Fixed (Aligned)
    let lane_fixed = UuRingLane::new_fixed("bench_fixed", size, 2).unwrap();
    lane_fixed.insert_batch(&data).unwrap();

    // Setup Flexible (Boxed slice)
    let lane_flex = UuRingLane::new_flexible("bench_flex", size, 2).unwrap();
    lane_flex.insert_batch(&data).unwrap();

    let mut group = c.benchmark_group("Cache Alignment Impact");

    group.bench_function("Fixed (64-byte Aligned)", |b| {
        b.iter(|| {
            let results = lane_fixed.curate_by(1, &following_users, 50);
            black_box(results);
        })
    });

    group.bench_function("Flexible (Boxed Slice)", |b| {
        b.iter(|| {
            let results = lane_flex.curate_by(1, &following_users, 50);
            black_box(results);
        })
    });

    group.finish();
    lane_fixed.vacate();
    lane_flex.vacate();
}

criterion_group!(benches, bench_comparison, bench_scenario, bench_alignment);
criterion_main!(benches);
