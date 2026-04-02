// Property-based tests for fork ancestry isolation.
//
// For deterministic fork integration tests (three-level chain) see fork_test.rs.
//
// `fork_ancestry_oracle`
//   Runs a sequence of pre-fork base ops (Write/Flush/Drain), takes a
//   snapshot, forks, then runs a mixed sequence of post-fork base and child
//   ops.  Two oracles are maintained:
//
//   - `base_oracle`: last value written to each LBA of the base fork.
//   - `child_oracle`: snapshot of base_oracle at fork time, then updated by
//     child writes only.  Post-branch base writes do NOT update it.
//
//   After every ChildCrash the test asserts:
//   1. Every LBA in child_oracle reads back the expected value.
//   2. Every LBA written to base *after* the fork (and not overwritten by the
//      child) reads back zero — post-branch base writes must be invisible.
//
//   After every BaseCrash the test asserts that every LBA in base_oracle reads
//   back correctly.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use elide_core::volume::{Volume, fork_volume};
use proptest::prelude::*;

mod common;

// --- SimOps ---

#[derive(Debug, Clone)]
enum PreOp {
    Write { lba: u8, seed: u8 },
    Flush,
    Drain,
}

#[derive(Debug, Clone)]
enum PostOp {
    BaseWrite { lba: u8, seed: u8 },
    BaseFlush,
    BaseDrain,
    ChildWrite { lba: u8, seed: u8 },
    ChildFlush,
    ChildDrain,
    ChildCrash,
    BaseCrash,
}

// --- strategies ---

fn arb_pre_ops() -> impl Strategy<Value = Vec<PreOp>> {
    prop::collection::vec(
        prop_oneof![
            (0u8..8, any::<u8>()).prop_map(|(lba, seed)| PreOp::Write { lba, seed }),
            Just(PreOp::Flush),
            Just(PreOp::Drain),
        ],
        1..20,
    )
}

fn arb_post_ops() -> impl Strategy<Value = Vec<PostOp>> {
    prop::collection::vec(
        prop_oneof![
            (0u8..8, any::<u8>()).prop_map(|(lba, seed)| PostOp::BaseWrite { lba, seed }),
            Just(PostOp::BaseFlush),
            Just(PostOp::BaseDrain),
            (0u8..8, any::<u8>()).prop_map(|(lba, seed)| PostOp::ChildWrite { lba, seed }),
            Just(PostOp::ChildFlush),
            Just(PostOp::ChildDrain),
            Just(PostOp::ChildCrash),
            Just(PostOp::BaseCrash),
        ],
        1..30,
    )
}

// --- property test ---

proptest! {
    #[test]
    fn fork_ancestry_oracle(pre_ops in arb_pre_ops(), post_ops in arb_post_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let by_id = dir.path().to_path_buf();
        let base_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let base_dir: PathBuf = by_id.join(base_ulid);
        let child_dir: PathBuf = by_id.join(child_ulid);

        // Write keypair into base_dir so Volume::open can load volume.key.
        std::fs::create_dir_all(&base_dir).unwrap();
        elide_core::signing::generate_keypair(
            &base_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let mut base = Volume::open(&base_dir, &by_id).unwrap();
        let mut base_oracle: HashMap<u64, [u8; 4096]> = HashMap::new();

        // --- pre-fork phase: build some base state ---
        for op in &pre_ops {
            match op {
                PreOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = base.write(*lba as u64, &data);
                    base_oracle.insert(*lba as u64, data);
                }
                PreOp::Flush => { let _ = base.flush_wal(); }
                PreOp::Drain => { common::drain_local(&base_dir); }
            }
        }

        // snapshot() flushes the WAL, then writes a snapshot marker.
        // fork_volume() requires at least one snapshot to exist.
        let _ = base.snapshot();
        fork_volume(&child_dir, &base_dir).unwrap();

        // child_oracle is the base state at the branch point.
        // Post-branch base writes must NOT be visible to the child.
        let mut child_oracle = base_oracle.clone();

        // LBAs written to base after the fork, never touched by the child.
        // These must read as zero from the child.
        let mut post_branch_base_lbas: HashSet<u64> = HashSet::new();

        let mut child = Volume::open(&child_dir, &by_id).unwrap();

        // --- post-fork phase ---
        for op in &post_ops {
            match op {
                PostOp::BaseWrite { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = base.write(*lba as u64, &data);
                    base_oracle.insert(*lba as u64, data);
                    // If this LBA is not in child_oracle (not a pre-branch write
                    // and not a child write), the child must continue to see zero.
                    if !child_oracle.contains_key(&(*lba as u64)) {
                        post_branch_base_lbas.insert(*lba as u64);
                    }
                    // child_oracle is NOT updated — post-branch base writes are invisible.
                }
                PostOp::BaseFlush => { let _ = base.flush_wal(); }
                PostOp::BaseDrain => { common::drain_local(&base_dir); }

                PostOp::ChildWrite { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = child.write(*lba as u64, &data);
                    child_oracle.insert(*lba as u64, data);
                    // If child explicitly writes this LBA, remove it from the
                    // "must be zero" set — the child now owns this LBA.
                    post_branch_base_lbas.remove(&(*lba as u64));
                }
                PostOp::ChildFlush => { let _ = child.flush_wal(); }
                PostOp::ChildDrain => { common::drain_local(&child_dir); }

                PostOp::ChildCrash => {
                    drop(child);
                    child = Volume::open(&child_dir, &by_id).unwrap();

                    // 1. Every child oracle LBA reads back correctly.
                    for (&lba, expected) in &child_oracle {
                        let actual = child.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "child lba {} wrong after crash+rebuild",
                            lba
                        );
                    }

                    // 2. LBAs written to base post-branch (never touched by child)
                    //    must be invisible — the child reads zeros.
                    for &lba in &post_branch_base_lbas {
                        let actual = child.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            [0u8; 4096].as_slice(),
                            "child lba {} should be zero (post-branch base write)",
                            lba
                        );
                    }
                }

                PostOp::BaseCrash => {
                    drop(base);
                    base = Volume::open(&base_dir, &by_id).unwrap();
                    for (&lba, expected) in &base_oracle {
                        let actual = base.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "base lba {} wrong after crash+rebuild",
                            lba
                        );
                    }
                }
            }
        }
    }
}
