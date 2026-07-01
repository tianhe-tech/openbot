# MCP Memory Tool

The gateway ships an MCP (Model Context Protocol) stdio server that exposes the
memory store directly to OpenCode's LLM as tools. This lets the LLM decide
**when** to search memory—eliminating the brittle keyword-based recall trigger.

## Build

```bash
go build -o bin/openbot-mcp ./cmd/mcp
```

## Configure opencode.json

Add the following to your project's `opencode.json` (typically at
`$OPENCODE_DIRECTORY/opencode.json`):

```jsonc
{
  "mcp": {
    "gateway-memory": {
      "type": "local",
      "command": ["./bin/openbot-mcp", "--db", "./tmp/memory.db"],
      "enabled": true
    }
  }
}
```

If the binary is installed globally or in a different path, adjust `command`
accordingly. The `--db` flag must point to the same SQLite memory database used
by the gateway (default: `$OPENCODE_DIRECTORY/tmp/memory.db`).

## Tools Exposed

| Tool | Description |
|------|-------------|
| `memory_search` | Full-text + fuzzy search over conversation history. Accepts `query` (required), optional `project`, `days`, `limit`. |
| `memory_list_projects` | List projects the user has worked on, with conversation counts and last-active timestamps. Optional `days` filter. |
| `memory_recent` | Retrieve recent conversations. Optional `days` (default 7), `limit` (default 20). |

## How It Works

```
OpenCode LLM  ──stdin/stdout──▶  openbot-mcp  ──SQLite──▶  memory.db
```

The server implements JSON-RPC 2.0 over stdio (the MCP "local" transport).
When the LLM calls a tool, the server queries the same SQLite FTS5 database
that the gateway writes to, returning formatted results.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MEMORY_DB_PATH` | (none) | Alternative to `--db` flag |

## Tips

- The LLM will see these tools in its tool list and can invoke them whenever
  it judges a memory lookup is relevant—no keyword trigger needed.
- Works alongside the existing `DetectRecallIntent` passive recall; both can
  coexist.
- The MCP server is read-only; it never writes to the database.
