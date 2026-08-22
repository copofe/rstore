//! The production startup path (CONTEXT.md: 启动编排).
//!
//! Service tests deliberately skip [`Application::initialize`] to stay
//! deterministic; this test is the one place the JOB-015 recovery rule runs
//! against the real production wiring: a job orphaned by a dead process must
//! come back as `interrupted`, while terminal states are left untouched.

use rstore_bootstrap::{Runtime, RuntimeConfig};
use rstore_domain::job::{Job, JobKind, JobRepository, JobState};
use rstore_domain::{Clock, JobId, SystemClock};

#[tokio::test]
async fn open_marks_jobs_orphaned_by_a_dead_process_interrupted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("catalog.db");
    let clock = SystemClock;

    // Simulate a dead process: one job left mid-flight (`Running`) and one
    // already-settled job (`Succeeded`) from an earlier run.
    let (orphan_id, settled_id) = {
        let db = rstore_persistence::Database::open(&db_path).await.unwrap();

        let mut orphan = Job::new(JobKind::Upload, JobId::new(), clock.now());
        orphan.transition(JobState::Running, clock.now()).unwrap();

        let mut settled = Job::new(JobKind::Download, JobId::new(), clock.now());
        settled.transition(JobState::Running, clock.now()).unwrap();
        settled
            .transition(JobState::Succeeded, clock.now())
            .unwrap();

        let repo = db.jobs();
        repo.create(&orphan).await.unwrap();
        repo.create(&settled).await.unwrap();
        (orphan.id, settled.id)
    };

    let runtime = Runtime::open(RuntimeConfig::new(db_path)).await.unwrap();
    let jobs = runtime.database().jobs().list().await.unwrap();

    let orphan = jobs.iter().find(|job| job.id == orphan_id).unwrap();
    assert_eq!(
        orphan.state,
        JobState::Interrupted,
        "a job orphaned by a dead process must be marked interrupted"
    );

    let settled = jobs.iter().find(|job| job.id == settled_id).unwrap();
    assert_eq!(
        settled.state,
        JobState::Succeeded,
        "terminal states must survive startup recovery untouched"
    );
}
