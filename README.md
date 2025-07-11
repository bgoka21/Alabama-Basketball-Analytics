# bball-analytics

This project analyzes basketball statistics and practice data for the Alabama team. The **Skill Development** tab relies solely on the `SkillEntry` model to track drills and NBA 100 entries. Earlier changes introducing an `Nba100Entry` model were reverted. Make sure any new code continues using `SkillEntry` for all skill-development features.

To run tests:

```bash
pytest -q
```
