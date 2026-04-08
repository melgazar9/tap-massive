from pathlib import Path

WORKFLOW_PATH = Path(".github/workflows/monitor-massive-changelog.yml")


def test_monitor_workflow_uses_tmp_for_cross_step_files() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "cron: '0 9 * * 1'" in workflow

    assert "/tmp/changelog-rss.xml" in workflow
    assert "/tmp/new-entries.json" in workflow

    assert (
        'curl -s "https://massive.com/changelog/rss.xml" > /tmp/changelog-rss.xml'
        in workflow
    )
    assert "ET.parse('/tmp/changelog-rss.xml')" in workflow
    assert "with open('/tmp/new-entries.json', 'w') as f:" in workflow
    assert "with open('/tmp/new-entries.json', 'r') as f:" in workflow

    # runner.temp is not available at job-level env, so we use /tmp directly
    assert "runner.temp" not in workflow
