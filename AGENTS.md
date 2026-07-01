# AGENTS.md — Guidance for LLM Agents

## Memory Recall

When the user asks about **past work, previous conversations, project history,
what they did before, or anything involving recall of prior context**, always
use the MCP memory tools first:

1. **`memory_search`** — search past conversations by keywords, project name,
   or time window. Use this for specific recall queries.
2. **`memory_list_projects`** — list projects the user has worked on. Use this
   when they ask "what projects have I worked on?" or similar.
3. **`memory_recent`** — get recent conversation summaries. Use when the user
   asks "what did I do recently?" or wants an overview.

**Do NOT** grep local directories, search filesystem, or guess based on file
names when the user is asking about past work. The memory tools have accurate,
structured records of all conversations.

### Examples of when to use memory tools

- "之前那个项目怎么做的？"
- "上次我们讨论了什么？"
- "帮我回忆一下上周做的事情"
- "我之前有没有做过类似的东西？"
- "What projects have I worked on?"
- "Remind me what we discussed about X"

### When NOT to use memory tools

- Current session questions (context is already available)
- Code generation that doesn't need historical context
- File operations on the current project
