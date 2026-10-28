# Factory Droid on T3 Code

This fork adds Factory Droid as a native T3 Code provider. T3 talks to Droid over Droid's own `stream-jsonrpc` protocol instead of ACP. That gives you Droid subagents in the lineage panel, approvals from subagents, `/compact` with a context meter, steering without restarting a turn, `$` skills, rollback through Droid's own rewind, and session import from the welcome wizard. The [Factory Droid provider guide](docs/user/providers-droid.md) covers provider setup and behavior.

Build from the `droid` branch. It merges every branch listed under [Branches in this build](#branches-in-this-build) on top of upstream `main`.

## Prerequisites

- Node 24 and `vp`, as described in the [development runbook](docs/operations/development.md).
- The Droid CLI, version 0.241.1 or newer. Run `droid` once in a terminal to sign in.

## Get the code

Clone this fork:

```sh
git clone https://github.com/jhomra21/t3code.git
cd t3code
git switch droid
```

Run `git pull` on `droid` to pick up later fixes.

To keep your own fork, fork `https://github.com/jhomra21/t3code` on GitHub and clone it instead. To bring in newer upstream T3 Code changes yourself:

```sh
git remote add upstream https://github.com/pingdotgg/t3code
git fetch upstream
git switch droid
git merge upstream/main
```

Resolve any conflicts and run the checks for the files involved before you build.

## Run from source

```sh
vp i
vp run dev           # server and web app
vp run dev:desktop   # Electron desktop app
```

Development state lives in `~/.t3/dev/userdata` for the main checkout and in `.t3/userdata` inside a linked worktree, so it stays apart from an installed T3 Code. Pass `--home-dir <dir>` to choose another location. Never point a development server at `~/.t3/userdata`.

## Build a macOS desktop app

```sh
node scripts/build-desktop-artifact.ts --platform mac --target dmg --arch arm64 --output-dir <dir>
```

A local build is not signed with a Developer ID, so macOS asks you to approve it the first time you open it. To sign it yourself, see [Desktop artifacts](docs/operations/development.md#desktop-artifacts).

If the build stops with `Expected @voidzero-dev/vite-plus-core@<version>`, your global `vp` is newer than the one this checkout pins. Put the checkout's own `vp` first on your `PATH` for the build:

```sh
PATH="$PWD/node_modules/.bin:$PATH" node scripts/build-desktop-artifact.ts --platform mac --target dmg --arch arm64 --output-dir <dir>
```

## Enable Factory Droid

Factory Droid is enabled by default unless you previously disabled it. To re-enable it, open **Settings → Providers**, select the environment that runs Droid, and enable **Factory Droid**. The Droid CLI must be installed and signed in on that environment's machine.

## Custom models and VibeProxy

T3's model picker lists the `customModels` from `~/.factory/settings.json` with the reasoning levels Droid reports for each one. T3 copies those model entries into its model-discovery sandbox without `apiKey` or other credential fields.

[VibeProxy](https://github.com/automazeio/vibeproxy) runs on your Mac and lets Droid use your Claude and ChatGPT subscriptions as custom models. Its [Factory setup guide](https://github.com/automazeio/vibeproxy/blob/main/FACTORY_SETUP.md) covers connecting accounts, model names, and the terms-of-service risk. That guide shows the older `~/.factory/config.json` format. T3 reads `customModels` from `~/.factory/settings.json`, so put the same models and URLs there:

```json
{
  "customModels": [
    {
      "model": "claude-haiku-5-5",
      "id": "custom:Claude Haiku 5.5-16",
      "index": 16,
      "displayName": "Claude: Haiku 5.5",
      "provider": "anthropic",
      "baseUrl": "http://localhost:8317",
      "apiKey": "dummy-not-used"
    },
    {
      "model": "gpt-6-luna",
      "id": "custom:GPT-6 Luna-12",
      "index": 12,
      "displayName": "GPT-6 Luna",
      "provider": "openai",
      "baseUrl": "http://localhost:8317/v1",
      "apiKey": "dummy-not-used"
    }
  ]
}
```

Use the model names your proxy serves. VibeProxy handles sign-in itself, so the API key is a placeholder. The `id` is the model id you pass to T3.

## Delegate subagents

Use T3's `delegate_task` tool to start child work that T3 tracks and can stop. Set `providerInstanceId` to `"droid"`, pick a model id from `orchestrator_capabilities`, and pass a reasoning level that model supports:

```json
{
  "task": "…",
  "mode": "async",
  "target": {
    "providerInstanceId": "droid",
    "model": "custom:GPT-6 Luna-12",
    "options": { "reasoning_effort": "high" }
  }
}
```

Each child picks its own model and effort, so a Claude parent can hand implementation to a GPT child and review to a Claude child.

## Branches in this build

Each branch solves one problem and has its own pull request on this fork. The session import and sidebar branches build on the provider branch.

| Branch                                             | What it does                                                                                       |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `feat/provider-droid-next`                         | Adds the native Droid provider with skills, rewind, context reporting, and live child transcripts. |
| `feat/droid-session-import`                        | Imports Factory Droid sessions from the welcome wizard.                                            |
| `fix/handoff-delivery-degrades-instead-of-failing` | Stops an oversized handoff from blocking a prompt when the thread still has room.                  |
| `fix/resumed-subagent-thread`                      | Keeps a resumed subagent's thread live after a server restart.                                     |
| `fix/pending-background-work-banner`               | Shows pending background work in the chat banner while the agent works.                            |
| `feat/sidebar-working-subagents`                   | Lists working subagents in the sidebar with their effort.                                          |
| `fix/child-effort-label`                           | Shows the right model and effort labels for native subagents.                                      |

## Limits

- T3's proposed-plan mode and text generation do not run through Droid.
- You install and sign in to the Droid CLI yourself, as with Claude Code.
