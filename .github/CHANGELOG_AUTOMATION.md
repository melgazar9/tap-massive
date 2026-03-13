# Massive API Changelog Automation

Automated monitoring of Massive API changes with AI-powered analysis and PR creation.

## How It Works

```
Weekly (Monday 9 AM UTC)
  → GitHub Action checks Massive RSS feed
  → New changes? → Claude reads changelog + all stream files + CLAUDE.md
  → Claude identifies schema updates needed
  → Creates branch + PR with schema updates
  → You review, test, merge
```

## Required Secrets

Add these in Repository Settings → Secrets → Actions:

| Secret | Source | Purpose |
|--------|--------|---------|
| `ANTHROPIC_API_KEY` | https://console.anthropic.com/ | Claude AI analysis |

**Note**: No Massive API key needed for changelog monitoring (uses public RSS feed)

## Workflow

### `monitor-massive-changelog.yml`
- **Trigger**: Weekly cron (Monday 9 AM UTC) or manual (`workflow_dispatch`)
- **What it does**:
  1. Fetches Massive RSS feed (https://massive.com/changelog/rss.xml), parses entries from last 21 days
  2. If new changes found, reads all stream files in `tap_massive/streams/` plus `CLAUDE.md`
  3. Sends changelog + full codebase context to Claude API
  4. Claude identifies affected schemas and generates updated files
  5. Creates a new branch and PR via GitHub API (never pushes to main)
- **No changes needed**: If Claude determines no code changes are required, the workflow ends with a summary note
- **Never pushes to main**

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
