# pebble-connector

Webhook receiver and event routing service for Pebble.

## Integration Test Note

This line was added as part of PEBBLE-894 integration test to verify git push and PR creation workflows through the proxy environment.

Commit 2: Testing continuous commit workflow.

Commit 3: Verifying push continues to work on repeated commits.

Commit 4: Testing Pebble MCP tool availability alongside git operations.

Commit 5: Backend DB was restarting during test - retrying Pebble MCP tools.

Commit 6: Continuing commit cycle. Pebble backend DB still recovering.
