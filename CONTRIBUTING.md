# Contributing to HydraDB

We use an issue-first flow, so that we can review each change. Read this page
before you open a pull request (PR).

Talk to us on [Discord](https://discord.gg/HcBKJHXmnu). Use the
`#pr-discussion` channel to ask about a PR, to ask for a review, or to ask us
to close a PR.

## The flow

```
open an issue  ->  issue /approved  ->  open a PR  ->  review & merge
   (anyone)         (maintainer)          (you)
```

1. Open an issue. Describe the bug or the change. Anyone can open an issue and discuss it.
2. Wait until a maintainer approves the issue with `/approved`.
3. Open a PR. Link the approved issue with `Closes #123`.
4. Keep the PR small. Change only what the issue asks for.

If a PR does not link an approved issue, a bot closes it.

A maintainer is a person in the `maintainers` list of
[`.github/vouched.json`](.github/vouched.json), or a person with write access
to this repository.

## Pull request rules

- Link an approved issue with `Closes #N`. If you do not, a bot closes the PR.
- Keep a maximum of 3 open PRs. If you open more, a bot closes your newest PR. Get an earlier PR merged or closed first. To ask, post in `#pr-discussion` on Discord.
- These two rules apply only to PRs opened after 2026-10-08. They do not apply to bot PRs. A maintainer can reopen a closed PR. The reopened PR gets the `gate-override` label, and the two rules do not apply to it.
- Keep each PR small. If a PR is large or has unrelated changes, we ask you to split it.
- If your PR has a breaking change, describe it in the PR.
- Fill in the "AI Assistance" section of the PR template. Do not remove it. If the section is missing, the gate check fails. You are responsible for each line that you submit.

## One PR, one thread

Each PR has one thread in the `#pr-discussion` channel on Discord. Keep all
discussion about a change in that thread and in the PR. Then the history is
easy to read.

## Build and test

For setup, see [`README.md`](README.md). For the workflow, see
[`DEVELOPMENT.md`](DEVELOPMENT.md). Run tasks with the `justfile`
(`just --list`), not with `cargo` directly.
