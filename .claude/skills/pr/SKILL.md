---
name: pr
description: Prepare outstanding commits on a feature branch for a pull request to upstream
---

Prepare a pull request from the current feature branch. This skill collects all commits on the current branch that are ahead of `upstream/main`, generates a PR title and description summarizing the changes, and creates the PR against `upstream`.

## Arguments

The skill accepts optional arguments:
- No arguments: auto-generate title and description from commits
- A short description: used as the PR title (e.g., `/pr Add support for TLS client certs`)

Example: `/pr` or `/pr Fix saturation search convergence`

## Steps

1. **Verify prerequisites**:
   - Must NOT be on `main` branch (should be on a feature branch)
   - The `upstream` remote must exist
   - The `origin` remote must exist

   ```bash
   BRANCH=$(git branch --show-current)
   if [ "$BRANCH" = "main" ]; then
     echo "Error: Must be on a feature branch, not main"
     exit 1
   fi
   git remote get-url upstream >/dev/null 2>&1 || { echo "Error: 'upstream' remote not found"; exit 1; }
   git remote get-url origin >/dev/null 2>&1 || { echo "Error: 'origin' remote not found"; exit 1; }
   ```

2. **Run `cargo fmt`** and amend the last commit if formatting changes are needed:
   ```bash
   cargo fmt
   if [ -n "$(git status --porcelain)" ]; then
     git add -u
     git commit --amend --no-edit
   fi
   ```

   After this step, the working directory must be clean. If there are still uncommitted changes (non-formatting), stop and tell the user to commit or stash them first.

3. **Sync with upstream**:
   ```bash
   git fetch upstream
   git fetch origin
   ```

4. **Identify commits for the PR**:
   - Find the merge base between the current branch and `upstream/main`
   - List all commits from the merge base to HEAD
   - Show the full diff against `upstream/main`

   ```bash
   MERGE_BASE=$(git merge-base upstream/main HEAD)
   git log --oneline ${MERGE_BASE}..HEAD
   git diff ${MERGE_BASE}..HEAD --stat
   ```

   If there are no commits ahead of upstream/main, stop and tell the user there is nothing to submit.

5. **Generate PR title and description**:
   - If the user provided a title argument, use that as the PR title
   - Otherwise, analyze the commits and diff to generate a concise PR title (under 70 characters)
   - Generate the PR body with:
     - A `## Summary` section with 1-3 bullet points describing the changes
     - A `## Changes` section listing the commits
     - A `## Test plan` section with a checklist of testing suggestions

   Present the title and body to the user for review before creating the PR.

6. **Push the feature branch to origin**:
   ```bash
   BRANCH=$(git branch --show-current)
   git push -u origin ${BRANCH}
   ```

7. **Create the PR against upstream**:
   ```bash
   BRANCH=$(git branch --show-current)

   gh pr create \
     --repo "$(git remote get-url upstream | sed 's/.*github.com[:/]\(.*\)\.git/\1/' | sed 's/.*github.com[:/]\(.*\)/\1/')" \
     --head "$(git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/' | sed 's/.*github.com[:/]\(.*\)/\1/' | cut -d/ -f1):${BRANCH}" \
     --base main \
     --title "${PR_TITLE}" \
     --body "${PR_BODY}"
   ```

8. **Report the PR URL** to the user.

## Troubleshooting

- **No `upstream` remote**: Add it with `git remote add upstream <repo-url>`
- **No `origin` remote**: This is your fork. Add with `git remote add origin <fork-url>`
- **gh CLI not installed**: `brew install gh` or see https://cli.github.com/
- **Not authenticated with gh**: `gh auth login`
- **PR already exists**: If a PR already exists for this branch, report the existing PR URL instead
