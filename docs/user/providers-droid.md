# Factory Droid

Factory Droid is a beta provider that runs on the machine hosting your T3 Code
environment. It uses Factory Droid's account, model catalog, and native session.

## Set up Factory Droid

Install Factory Droid on the environment's machine and sign in there using
Factory Droid. In T3 Code, open **Settings → Providers**, select that
environment, enable **Factory Droid**, and refresh the provider. Droid is
disabled until you enable it. T3 Code checks the `droid` executable and version,
verifies sign-in, and loads the available models.
If `droid` is not on the server's `PATH`, set **Binary path** to its executable.

T3 Code does not install Factory Droid, but it can update it. When a newer
release is out, the provider card shows it, and **Update now** runs
`droid update` or the package manager that installed Droid. See
[Providers](./install.md#providers). If the provider check cannot
verify authentication or discover models, refresh after signing in and confirm
that the configured binary runs on the environment's machine.

## Start a thread

Choose **Factory Droid** in the thread's provider/model picker and send a
message. T3 Code creates a native Droid session and resumes it for later
messages in that thread. You can switch models within the same session.

## Models and options

T3 Code discovers enabled models from Factory Droid. Choose a model in the
picker, or leave the selection on **Auto**. If the catalog does not include
Droid's default model, no model is sent when updating session settings, so the
previously selected model remains active. You can add a **Custom model** in the provider instance
settings when you need a model that is not in the discovered list.

Models that report reasoning-effort choices show a **Reasoning** option. The
selected model's supported efforts and default come from Droid.
Available Droid skills can be invoked from the composer with `$`.

T3 Code maps its [permission modes](./permission-modes.md) to Droid autonomy:
**Supervised** uses `off`, **Auto-accept edits** uses `low`, **Auto** uses
`medium`, and **Full access** uses `high`. Droid enforces the selected autonomy;
**Plan** mode is not available. Autonomy follows the thread's permission mode
alone, so there is no separate autonomy setting. A helper that an agent starts
from a Droid thread inherits that mode and cannot be given a broader one.

## Approvals, questions, and subagents

Droid can ask for approval before commands or file changes and can ask questions
in the conversation. Approval choices come from Droid, including one-time
approval, session approval when offered, and cancellation. Pending approvals
are live requests: if the provider session stops, they cannot be answered later.
Droid does not request approval for file reads.

Droid subagents can request approvals or ask questions. T3 Code follows their
session transcripts to report their activity. T3 Code cannot wait for or close
a subagent independently.

## Limits

Import an existing Droid session from the welcome wizard. Conversation rollback
uses Droid's native rewind; forking a thread is not supported. Droid can finish
background tasks after a turn; T3 Code brings their completion back into the
thread. The context meter shows Droid's context window and usage, and
**Compact context** (or `/compact`) compacts the session. Droid also compacts
on its own, and each compaction leaves a **Context compacted** item in the
thread. Switching to another provider can still pass conversation context
through a portable handoff.

The sign-in-link reminder is prompt-based: it is appended to new sessions and
is not added when resuming an existing session.

When the model is rate limited or out of quota, Droid retries on its own. T3
Code shows each retry, then ends the turn with **Usage limit reached** and
Droid's message. Droid can wait a very long time on a large `Retry-After`; stop
the turn if you do not want to wait. Other failures show Droid's message, or
**“Droid did not complete the turn.”** when it gives none.
