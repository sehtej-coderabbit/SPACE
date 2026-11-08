//! A start's runtime upgrade pod against faults of the Kubernetes API and of
//! the execution's record: a pod delete the API refuses, a pod read it never
//! answers, a create whose answer it loses, and a settled pod a later
//! attempt's joiner must not take for its owner's.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::*;

/// The node keeps reporting a settled pod's rollback or refusal after the
/// activation deleted the pod, until a new pod's shim reaches it, and the
/// execution still records the settled pod's UID once the activation gives
/// itself up. A later attempt claims the execution and creates its own pod at
/// the same name only after its materialization clears that record, so a
/// start joining that attempt before the create finds the settled pod
/// recorded and no pod there. It never saw that pod, so it doesn't read it as
/// its owner's pod gone and settle on the earlier attempt's outcome while its
/// owner is about to boot the upgrade: it waits to its deadline.
#[rstest::rstest]
#[case::rolled_back(Report::RolledBack)]
#[case::refused(Report::Refused)]
#[tokio::test(flavor = "current_thread")]
async fn a_settled_pod_is_never_a_later_attempts_owner_pod(
    #[case] report: Report,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    let pool = db.pool.clone();
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.node.report(report);

    let error = start(&substrate, sandbox_id)
        .await
        .expect_err("the activation settled");
    assert!(
        matches!(
            error,
            SubstrateError::Unavailable { .. } | SubstrateError::FailedPrecondition { .. }
        ),
        "{error:?}"
    );
    assert!(!cluster.pods.exists(), "the settled pod is still there");
    let settled_uid = cluster.pods.created_uids()[0].clone();
    let recorded: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(recorded.as_deref(), Some(settled_uid.as_str()));

    // A later attempt claims the execution and hasn't created its pod yet.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = gen_random_uuid(),
                activation_attempt_target_state = 'started',
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    let error = start_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the joiner settled");

    assert!(
        matches!(error, SubstrateError::Timeout { .. }),
        "the joiner settled on settled pod {settled_uid}'s outcome: {error:?}"
    );
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1);
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}

/// A rollback the node recorded for the start's pod settles only once that
/// pod is deleted under its UID, since kubelet would otherwise retry its
/// create forever. A delete the API refuses for its UID precondition says
/// another pod holds the name, which may be booting the upgrade, so nothing
/// settles and the start runs to its deadline with the pod kept. A delete
/// that fails outright doesn't hold the outcome back: the start settles and
/// leaves the pod for its retry, whose wait settles on the same report and
/// deletes it then.
#[rstest::rstest]
#[case::another_pod_holds_the_name(true, false)]
#[case::the_delete_fails(false, true)]
#[tokio::test(flavor = "current_thread")]
async fn a_settled_pod_settles_only_on_its_own_delete(
    #[case] precondition_fails: bool,
    #[case] settles: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.node.report(Report::RolledBack);
    let refusals = if precondition_fails {
        &cluster.pods.precondition_failing_deletes
    } else {
        &cluster.pods.failing_deletes
    };
    refusals.store(usize::MAX, Ordering::SeqCst);

    let error = start_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the start settled as a success");

    if settles {
        assert!(
            matches!(&error, SubstrateError::Unavailable { message } if message.contains("rolled back")),
            "{error:?}"
        );
    } else {
        assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    }
    assert!(cluster.pods.exists(), "the API deleted the pod after all");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}

/// Each look at the upgrade's outcome bounds its reads by the wait's
/// deadline, with a floor of `NODE_READ_FLOOR`, so an API server that takes
/// the pod read and never answers holds the start at most that floor past
/// its deadline, plus one poll, and the start answers its retryable timeout.
#[tokio::test(flavor = "current_thread")]
async fn a_pod_read_that_never_answers_holds_the_wait_at_most_the_floor() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    cluster.pods.pod_reads_hang.store(true, Ordering::SeqCst);
    let ready_timeout = Duration::from_millis(800);

    let started = std::time::Instant::now();
    let error = start_within(&substrate, sandbox_id, ready_timeout)
        .await
        .expect_err("the start settled");
    let took = started.elapsed();

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    let bound = ready_timeout
        + crate::controllers::substrate::kubernetes::pod_runtime_upgrade::NODE_READ_FLOOR
        + Duration::from_millis(700);
    assert!(took < bound, "the start took {took:?}, past {bound:?}");
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A create carrying the upgrade whose answer the API lost, with no pod at
/// the name on the confirming read, isn't a clean failure: the API server may
/// still commit it, and its shim would then boot the upgrade. So the start
/// answers retryably and keeps its attempt's claim on the execution until the
/// lease ends, rather than giving it up for another activation to take.
#[tokio::test(flavor = "current_thread")]
async fn a_lost_upgrade_create_keeps_its_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster
        .pods
        .creates_fail_uncommitted
        .store(true, Ordering::SeqCst);

    let error = start(&substrate, sandbox_id)
        .await
        .expect_err("the start succeeded without a pod");

    assert!(
        matches!(&error, SubstrateError::Unavailable { message } if message.contains("may still land")),
        "{error:?}"
    );
    assert!(!cluster.pods.exists());
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    assert!(
        matches!(live_claim(&db.pool, sandbox_id).await?, Some((_, true))),
        "the start gave its claim up"
    );
    Ok(())
}

/// A legacy resident execution, one that predates route identities, is
/// adopted only while it records a pod UID: an assignment refuses a
/// route-less row that records none. So a settled pod's UID stays on such a
/// row, and the next start adopts it again rather than finding the execution
/// unclaimable.
#[tokio::test(flavor = "current_thread")]
async fn a_settled_pod_on_a_legacy_execution_keeps_it_claimable() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    let pool = db.pool.clone();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = TRUE, state = 'created', route_activation_id = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.node.report(Report::RolledBack);

    let error = start(&substrate, sandbox_id)
        .await
        .expect_err("the start settled as a success");
    assert!(
        matches!(&error, SubstrateError::Unavailable { message } if message.contains("rolled back")),
        "{error:?}"
    );
    assert!(!cluster.pods.exists(), "the settled pod is still there");

    cluster.pods.run_containers();
    cluster.node.report(Report::Upgraded { publish: false });
    let retry = start(&substrate, sandbox_id).await.map(drop);

    assert!(
        !matches!(&retry, Err(SubstrateError::Unavailable { message }) if message.contains("route-less")),
        "the settled pod left the execution unclaimable: {retry:?}"
    );
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 2);
    Ok(())
}

/// A retry's create that conflicts with a pod at the name carrying the
/// upgrade, one an earlier attempt left and that's now stale (failed), isn't
/// a clean failure when the stale pod can't be deleted: the pod's create call
/// may still be on its way to the node. So the start keeps its attempt's
/// claim until the lease ends rather than giving it up for another
/// activation to take.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_upgrade_pod_that_cant_be_deleted_keeps_the_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    let pool = db.pool.clone();
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    // An earlier attempt placed the upgrade's pod and ran out its wait.
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    let earlier = start_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the earlier attempt settled");
    assert!(
        matches!(earlier, SubstrateError::Timeout { .. }),
        "{earlier:?}"
    );
    // Its pod has failed since, its lease has ended, and the API refuses to
    // delete the pod.
    if let Some(pod) = cluster.pods.pod.lock().unwrap().as_mut() {
        pod["status"]["phase"] = serde_json::json!("Failed");
    }
    cluster.pods.end_container();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    cluster
        .pods
        .failing_deletes
        .store(usize::MAX, Ordering::SeqCst);

    let error = start(&substrate, sandbox_id)
        .await
        .expect_err("the retry succeeded");

    assert!(cluster.pods.exists(), "the stale pod was deleted after all");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    assert!(
        matches!(live_claim(&pool, sandbox_id).await?, Some((_, true))),
        "the retry gave its claim up: {error:?}"
    );
    Ok(())
}

/// The same stale upgrade pod, found in the pod reflector's cache rather than
/// on a conflicting create: the retry confirms it with the API, finds it
/// stale and can't delete it. It may still be running the upgrade, so the
/// retry keeps its attempt's claim.
#[tokio::test(flavor = "current_thread")]
async fn a_cached_stale_upgrade_pod_that_cant_be_deleted_keeps_the_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    let pool = db.pool.clone();
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    let earlier = start_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the earlier attempt settled");
    assert!(
        matches!(earlier, SubstrateError::Timeout { .. }),
        "{earlier:?}"
    );
    if let Some(pod) = cluster.pods.pod.lock().unwrap().as_mut() {
        pod["status"]["phase"] = serde_json::json!("Failed");
    }
    cluster.pods.end_container();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    cluster
        .pods
        .failing_deletes
        .store(usize::MAX, Ordering::SeqCst);
    let mut retrying = cluster.substrate_caching_the_pod(&pool);
    retrying.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let error = start(&retrying, sandbox_id)
        .await
        .expect_err("the retry succeeded");

    assert!(cluster.pods.exists(), "the stale pod was deleted after all");
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1, "{error:?}");
    assert!(
        matches!(live_claim(&pool, sandbox_id).await?, Some((_, true))),
        "the retry gave its claim up: {error:?}"
    );
    Ok(())
}

/// A resume whose pod carries an earlier resume's upgrade, one it no longer
/// renders (here the decision is off now), keeps that pod, since the upgrade
/// may have retired the checkpoint it read. So that upgrade's outcome decides
/// the resume: a refusal the node recorded for the pod settles it, the pod is
/// deleted so a retry places a new one, and no Wake goes to a pod that will
/// never run, rather than every resume waiting on it.
#[tokio::test(flavor = "current_thread")]
async fn a_kept_restore_pod_settles_on_the_upgrade_it_carries() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    let (mut substrate, cluster) = user_restore_subject(&pool, sandbox_id).await?;
    block_e0_for_e1(&pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    // The earlier resume placed the upgrade's pod and ran out its wait.
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    let earlier = restore_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the earlier resume settled");
    assert!(
        matches!(
            earlier,
            SubstrateError::Timeout { .. } | SubstrateError::RestoreInFlight { .. }
        ),
        "{earlier:?}"
    );
    assert!(cluster.pods.exists());
    // Its leases have ended, the node has refused the upgrade for that pod,
    // and the decision no longer renders an upgrade.
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second',
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    cluster.node.report(Report::Refused);
    // The pod reflector has held the kept pod since the earlier resume.
    let mut substrate = cluster.substrate_caching_the_pod(&pool);
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Disabled;

    let error = restore(&substrate, sandbox_id)
        .await
        .expect_err("the resume woke a pod that never runs");

    assert!(
        matches!(&error, SubstrateError::FailedPrecondition { message } if message.contains("was refused before it began")),
        "{error:?}"
    );
    assert!(!cluster.pods.exists(), "the refused pod is still there");
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1);
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}

/// A resume whose pod carries an earlier resume's upgrade settles on it even
/// when its own decision now refuses: here the catalog has since lost the
/// edge, so the sandbox is stranded. Refusing first would leave the kept pod
/// unread, its recorded refusal never settled and the pod never deleted.
#[tokio::test(flavor = "current_thread")]
async fn a_kept_restore_pod_settles_even_once_the_sandbox_is_stranded() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    let (mut substrate, cluster) = user_restore_subject(&pool, sandbox_id).await?;
    block_e0_for_e1(&pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    let earlier = restore_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the earlier resume settled");
    assert!(
        matches!(
            earlier,
            SubstrateError::Timeout { .. } | SubstrateError::RestoreInFlight { .. }
        ),
        "{earlier:?}"
    );
    assert!(cluster.pods.exists());
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second',
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    // E0 has lost its certified edge, so a fresh decision strands it.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = '{}' WHERE environment_ref = $1"
    ))
    .bind(E0)
    .execute(&pool)
    .await?;
    cluster.node.report(Report::Refused);
    let mut substrate = cluster.substrate_caching_the_pod(&pool);
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let error = restore(&substrate, sandbox_id)
        .await
        .expect_err("the resume woke a pod that never runs");

    assert!(
        matches!(&error, SubstrateError::FailedPrecondition { message } if message.contains("was refused before it began")),
        "{error:?}"
    );
    assert!(!cluster.pods.exists(), "the refused pod is still there");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}

/// A start that joins another attempt's claim reads its owner's pod from the
/// API, not a cache. Here a cache still holds an earlier attempt's pod, under
/// the UID the execution still records, though the API has deleted it, and
/// the node still reports that pod's rollback. Taken for the owner's pod, it
/// would settle the joiner on the earlier attempt's outcome, deleting a pod
/// that's gone; read from the API, there's no pod yet, and the joiner waits
/// to its deadline for its owner's.
#[tokio::test(flavor = "current_thread")]
async fn a_joiner_never_takes_a_cached_earlier_pod_for_its_owners() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, mut substrate, cluster) = start_subject("stopped", false).await?;
    let pool = db.pool.clone();
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    // An earlier attempt placed the upgrade's pod and ran out its wait.
    cluster.pods.keep_creating();
    cluster.node.report(Report::InProgress);
    let earlier = start_within(&substrate, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the earlier attempt settled");
    assert!(
        matches!(earlier, SubstrateError::Timeout { .. }),
        "{earlier:?}"
    );
    // A cache still holds that pod; the API has deleted it, the node reports
    // its rollback, and a new owner has claimed the execution and not yet
    // created its own pod.
    let mut joiner = cluster.substrate_caching_the_pod(&pool);
    joiner.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    *cluster.pods.pod.lock().unwrap() = None;
    cluster.node.report(Report::RolledBack);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = gen_random_uuid(),
                activation_attempt_target_state = 'started',
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    let deletes = cluster.pods.deletes.load(Ordering::SeqCst);

    let error = start_within(&joiner, sandbox_id, Duration::from_millis(800))
        .await
        .expect_err("the joiner settled");

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    assert_eq!(
        cluster.pods.deletes.load(Ordering::SeqCst),
        deletes,
        "the joiner acted on the cached earlier pod"
    );
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}
