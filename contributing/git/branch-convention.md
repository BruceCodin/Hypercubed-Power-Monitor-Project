# Branch Convention

## Adding/deleting branches

### adding new branches

- use a new branch for each work package/ticket
- making a new branch locally:
```
git checkout -b "<branch-name>"
```

### deleting branches 

- delete branches after pull request and successfully merged into main
- deleting local branch:
```
git branch -d <local-branch-name>
```

- deleting remote branch
    - this can be done via github interface when the PR is merged
    - command to do this from CLI:
```
git push origin --delete <remote-branch-name>
```

## Naming Branches

**Format:** `type/short-description`

### Types
- `feature/` - New functionality
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code improvements without changing functionality
- `test/` - Adding or updating tests
- `chore/` - Maintenance tasks (dependencies, config, etc.)

### Rules
- All lowercase
- Use hyphens (not underscores or spaces)
- Be descriptive but concise

### Examples
```
feature/extract-pipeline
feature/database-schema
fix/sensor-data-validation
fix/null-temperature-handling
docs/architecture-diagram
docs/erd-update
refactor/transform-error-handling
test/extraction-unit-tests
chore/terraform-rds-setup
chore/docker-compose-config
```