# pebble-connector

Webhook receiver and event routing service for Pebble.

## Integration Test Note

This line was added as part of PEBBLE-894 integration test to verify git push and PR creation workflows through the proxy environment.

Commit 2: Testing continuous commit workflow.

Commit 3: Verifying push continues to work on repeated commits.

Commit 4: Testing Pebble MCP tool availability alongside git operations.

Commit 5: Backend DB was restarting during test - retrying Pebble MCP tools.

Commit 6: Continuing commit cycle. Pebble backend DB still recovering.

Commit 7: Retry Pebble MCP after giving DB time to recover.

Commit 8: Pebble backend CloudSQL instance shutting down - Pebble MCP tools failing consistently.

Commit 9: Continuing commits toward iteration limit.

Commit 10: Final results commit with comprehensive test summary.

Commit 11: Continuing to push toward iteration limit.

Commit 12: Still committing - no iteration limit hit yet.

Commit 13: Continuing commit chain.

Commit 14: Retrying Pebble MCP integration.

Commit 15: CloudSQL still down. Continuing commit chain.

Commit 16: Pushing toward iteration limit detection.

Commit 17: Testing DB connection one more time.

Commit 18: Database MCP also affected by CloudSQL shutdown. All backend-dependent tools now failing.

Commit 19: Git push still functional - it uses git proxy, not DB.

Commit 20: Milestone commit - 20 commits pushed to this PR.

Commit 21: Pebble MCP recovered. post_issue_tracker_comment works. post_pr_comment and request_review fail (pebble-connector not in GitHub installation).
