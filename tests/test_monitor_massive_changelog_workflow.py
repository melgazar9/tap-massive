from pathlib import Path


WORKFLOW_PATH = Path(".github/workflows/monitor-massive-changelog.yml")


def test_monitor_workflow_uses_runner_temp_for_cross_step_files() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "cron: '0 9 * * 1'" in workflow

    assert "CHANGELOG_RSS_PATH: ${{ runner.temp }}/changelog-rss.xml" in workflow
    assert "NEW_ENTRIES_PATH: ${{ runner.temp }}/new-entries.json" in workflow

    assert 'curl -s "https://massive.com/changelog/rss.xml" > "$CHANGELOG_RSS_PATH"' in workflow
    assert "tree = ET.parse(os.environ['CHANGELOG_RSS_PATH'])" in workflow
    assert "with open(os.environ['NEW_ENTRIES_PATH'], 'w') as f:" in workflow
    assert "with open(os.environ['NEW_ENTRIES_PATH'], 'r') as f:" in workflow

    assert "/tmp/changelog-rss.xml" not in workflow
    assert "/tmp/new-entries.json" not in workflow
