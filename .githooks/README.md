# `.githooks/` — version-controlled git hooks

This directory holds the git hooks shipped with the repo. Hooks are
*not* installed automatically (git checkouts don't run them), so each
developer opts in once:

```sh
make install-hooks
```

That sets `core.hooksPath` to `.githooks` for this clone, so subsequent
commits and pushes run the hooks below. Repository-local config — no
global side effects.

## Hooks

- **`pre-commit`** runs `make lint` (~10 s warm). Matches CI's lint
  gate, catches issues at commit time. Bypass with `git commit
  --no-verify`.
- **`pre-push`** runs `make verify` (vet + lint + test, ~30-45 s warm).
  This is the canonical CI gate run locally; if it passes, CI passes.
  Bypass with `git push --no-verify`.

## Why hook-via-`core.hooksPath` rather than scripting `.git/hooks/`

`.git/hooks/` is per-clone and not under version control. Putting the
hooks in `.githooks/` and pointing `core.hooksPath` at it means the
hooks live with the code, get reviewed in PRs, and stay in sync across
the team. The one-time `make install-hooks` is the only manual step.

## Why a pre-commit *and* a pre-push hook

The pre-push hook is the canonical CI mirror — if it passes, CI passes.
But its ~30 s runtime is too slow to want on every `git commit`. The
pre-commit lint catches the most common gate failure (lint errors) in
~10 s so the iteration loop stays tight; pre-push catches everything
else.

If you bypass pre-commit with `--no-verify`, pre-push still catches
the failure before the push leaves your machine. Belt and suspenders.
