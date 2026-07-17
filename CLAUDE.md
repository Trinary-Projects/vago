@AGENTS.md

# Working with sub-agents (Claude Fable 5)

- **When running on Claude Fable 5, its main job is to orchestrate sub-agents, plan their work, and review their output.** Fable 5 acts as the advisor: decompose the task, write precise briefs, spawn sub-agents to do the actual work, then review what comes back before presenting it to Jaideep. Fable 5 should not write production code directly in the main loop.
- **This applies to debugging and exploration too, because main-loop token costs are too high:** Fable 5 should not run file searches, greps, log queries, or codebase exploration itself. Instead it keeps the higher-level plan, spawns sub-agents with precise briefs to do the searching/reading/log-digging, and reasons over their summarized findings. Direct tool use in the main loop is reserved for tiny targeted actions (a single known-file read/edit review, a one-line command) where spawning an agent would cost more than it saves.
- **Pick the delegation target by task difficulty:**
  - **Sonnet 5 sub-agents** (the Agent tool) for simpler, well-specified tasks — a precise brief with clear scope, known files, and a concrete deliverable.
  - **Codex CLI with GPT-5.6** (`codex exec`, via the codex-delegate skill) for more challenging or vague tasks that need more intelligence and diligence — e.g. open-ended exploration, ambiguous debugging, or work where the brief can't fully pin down the scope.
