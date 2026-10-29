# Factory Droid on T3 Code

This fork adds Factory Droid as a native T3 Code provider. T3 talks to Droid over Droid's own `stream-jsonrpc` protocol instead of ACP. That gives you Droid subagents in the lineage panel, approvals from subagents, `/compact` with a context meter, steering without restarting a turn, `$` skills, rollback through Droid's own rewind, and session import from the welcome wizard. The [Factory Droid provider guide](docs/user/providers-droid.md) covers provider setup and behavior.

The `droid` branch is the rolling, tested fork build: it combines a pinned upstream T3 Code nightly with the Droid provider and selected fork fixes. It is not a recipe to merge every historical branch. The current source baseline is commit `70683231a392f7b255af13172d1efef82f012982`, based on upstream nightly `v0.1.5-nightly.20261029.3877` at `6d450a4284852af137b2f3407fddd89eb76e7644`. The documentation update here does not change that runtime baseline. To reproduce it independently of the rolling branch, check out the source commit:

```sh
git switch --detach 70683231a392f7b255af13172d1efef82f012982
```

## Prerequisites

- Node `26.12.1` and `vp`, as pinned in the root `package.json` and described in the [development runbook](docs/operations/development.md).
- The Droid CLI, version 0.241.1 or newer. Run `droid` once in a terminal to sign in.

## Get the code

Clone this fork:

```sh
git clone https://github.com/jhomra21/t3code.git
cd t3code
git switch droid
```

Update a checkout of the published `droid` branch with a fast-forward only:

```sh
git pull --ff-only
```

If your checkout has diverged, inspect and preserve your work before updating it. Do not reset or
stash it blindly.

To keep your own fork, fork `https://github.com/jhomra21/t3code` on GitHub and clone it instead.
To update the fork from a chosen upstream nightly, fetch its tags and merge the exact tag commit
into `droid`, then resolve conflicts, validate, and review the resulting build before publishing:

```sh
git remote add upstream https://github.com/pingdotgg/t3code
git fetch upstream --tags
git switch droid
git merge 6d450a4284852af137b2f3407fddd89eb76e7644
```

The commit above is the upstream base for the current baseline; choose and verify a newer tagged
nightly before using this procedure for a later update. Do not merge unpinned `upstream/main`.

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

A local macOS build may be ad-hoc signed or unsigned, and is not Developer ID signed or notarized.
Check the app signature before installing:

```sh
codesign --verify --deep --strict "/path/to/T3 Code.app"
```

An ad-hoc or unsigned artifact may fail strict verification; a macOS approval prompt does not
replace a valid signature. Development signing can help with local testing, but is not Developer
ID signing or notarization. See [Desktop artifacts](docs/operations/development.md#desktop-artifacts)
for build options.

If the build stops with `Expected @voidzero-dev/vite-plus-core@<version>`, your global `vp` is newer than the one this checkout pins. Put the checkout's own `vp` first on your `PATH` for the build:

```sh
PATH="$PWD/node_modules/.bin:$PATH" node scripts/build-desktop-artifact.ts --platform mac --target dmg --arch arm64 --output-dir <dir>
```

## T3 Connect in a self-built app

A packaged build only shows T3 Connect sign-in if the T3 Connect public settings are present at build time. Put `T3CODE_CLERK_PUBLISHABLE_KEY`, `T3CODE_CLERK_JWT_TEMPLATE`, `T3CODE_CLERK_CLI_OAUTH_CLIENT_ID` and `T3CODE_RELAY_URL` in the gitignored `.env` at the repository root, or export them in the shell, before you build. The values come from the T3 Connect deployment you use. A fresh clone or worktree has no `.env`. A build without these settings starts normally, but T3 Connect stays in a `session-pending` state.

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

Choose the child model and supported effort from `orchestrator_capabilities` using the available
provider/model catalog. The parent passes them in `target`; a child does not choose its own model
or effort. For linked environments, enable **Settings → Labs → Linked environments** on both
machines, link them under **Settings → Connections**, and pass the destination `environmentId` as
`target.environmentId`. See the [remote access guide](docs/user/remote-access.md).

The current build combines upstream peer/delegation support with the fork's native Droid provider
and selected fixes, including Windows SDK compatibility, handoff delivery, and agent-session
scanning. Older feature-branch names are historical references, not a build recipe.

## Limits

- T3's proposed-plan mode and text generation do not run through Droid.
- You install and sign in to the Droid CLI yourself, as with Claude Code.
