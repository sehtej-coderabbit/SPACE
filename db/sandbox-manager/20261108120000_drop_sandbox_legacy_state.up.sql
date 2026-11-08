-- Every reader decides from `state` and `cleanup_state`, so the combined
-- status the cleanup-state split kept for them goes.
ALTER TABLE sandboxes DROP COLUMN legacy_state;
