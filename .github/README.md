# GitHub Configuration

Configuration and templates for project automation, CI/CD, and contributor guidelines.

## Contents

| Item | Purpose |
|------|---------|
| [workflows/](workflows/) | GitHub Actions CI/CD pipelines |
| [ISSUE_TEMPLATE/](ISSUE_TEMPLATE/) | Issue templates for bugs, features, and tasks |
| [pull_request_template.md](pull_request_template.md) | PR template for standardized submissions |

## Workflows

**python_ci.yml** - Linting and testing pipeline

- Runs on: push to main, pull requests
- Lints Python files with pylint (e8.0 threshold)
- Runs pytest with coverage for all test files

## Issue Templates

- **bug_report.md** - Report bugs with reproduction steps
- **feature_request.md** - Propose features with user stories and benefits
- **task.md** - Track general tasks and improvements

## PR Guidelines

All PRs require:
- Type classification (feature, bug fix, docs, refactor, chore)
- Testing details and coverage info
- Related issue references
- Pylint score >= 8.0
- All Pytests passing
