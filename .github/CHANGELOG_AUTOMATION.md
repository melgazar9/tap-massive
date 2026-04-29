# Massive API Changelog Automation

Automated monitoring of Massive API changes with AI-powered analysis and PR creation.

## How It Works

```
Weekly (Monday 9 AM UTC)
  → GitHub Action checks Massive RSS feed
  → New changes? → anthropics/claude-code-action runs Claude Code in CI:
      • Reads CLAUDE.md, tap_massive/*.py, formatted changelog
      • Classifies entries (new endpoint / new fields / bug fix / deprecation)
      • Edits stream code + meltano.yml
      • Runs ./lint.sh and self-reviews diff for DRY/SOLID/duplication
      • Creates branch, commits, pushes, opens PR via gh
  → You review, test, merge
```

## Required Secrets

Add these in Repository Settings → Secrets → Actions:

| Secret | Source | Purpose |
|--------|--------|---------|
| `CLAUDE_TAP_MASSIVE_API_KEY` | https://console.anthropic.com/ | Claude Code analysis |

**Note**: No Massive API key needed for changelog monitoring (uses public RSS feed)

## Required Repository Settings

Settings → Actions → General → Workflow permissions:
- **"Allow GitHub Actions to create and approve pull requests"** must be checked (required for the workflow to open PRs).

## Workflow

### `monitor-massive-changelog.yml`
- **Trigger**: Weekly cron (Monday 9 AM UTC) or manual (`workflow_dispatch`)
- **What it does**:
  1. Fetches Massive RSS feed (https://massive.com/changelog/rss.xml), parses entries from last 21 days
  2. If new changes found, sets up `uv` and installs project dependencies
  3. Runs `anthropics/claude-code-action@v1` with full read/write access to the working tree
  4. Claude reads `CLAUDE.md` + stream files, edits code, runs `./lint.sh`, self-reviews, then opens a PR
- **No changes needed**: If Claude classifies all entries as bug fixes / deprecations, no PR is opened
- **Never pushes to main**
- **Cost guard**: `timeout-minutes: 30` and `--max-turns 30` cap a runaway run

## Customization

### Change check frequency
Edit the cron in `monitor-massive-changelog.yml`:
```yaml
schedule:
  - cron: '0 9 * * 1'      # Weekly Monday (default)
  - cron: '0 9 * * *'      # Daily
  - cron: '0 9 * * 1,5'    # Monday + Friday
```

### Change lookback window
Edit the `21 days ago` value in `monitor-massive-changelog.yml`.

## Troubleshooting

- **Workflow not running**: Verify ANTHROPIC_API_KEY secret is set and workflow is on `main` branch
- **Claude returns invalid JSON**: Workflow fails with error; check the Actions run log for the raw response
- **RSS feed fails**: Check https://massive.com/changelog/rss.xml is accessible
- **Duplicate PRs**: The workflow runs weekly; if the same changelog entries are still within the 21-day window, a new PR may be created. Close or merge existing PRs to keep things tidy.
