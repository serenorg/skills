# SaaS Short Strategy Bot

Production-oriented autonomous skill for shorting AI-vulnerable SaaS equities.

## Directory

```
seren/saas-short-strategy-bot/
├── SKILL.md
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── config.example.json
└── scripts/
    ├── dry_run_checklist.md
    ├── dry_run_prompt.txt
    ├── run_agent_server.py
    ├── setup_cron.py
    ├── setup_serendb.py
    ├── strategy_engine.py
    ├── serendb_storage.py
    ├── seren_client.py
    ├── self_learning.py
    ├── serendb_schema.sql
    └── self_learning_schema.sql
```

## Quick Start

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
cp config.example.json config.json
python3 scripts/setup_serendb.py --dsn "$SERENDB_DSN"
python3 scripts/strategy_engine.py --dsn "$SERENDB_DSN" --run-type scan --mode paper-sim --strict-required-feeds --config config.json
```

## Continuous Operation

```bash
SERENDB_DSN="$SERENDB_DSN" SAAS_SHORT_BOT_WEBHOOK_SECRET="$SAAS_SHORT_BOT_WEBHOOK_SECRET" \
python3 scripts/run_agent_server.py --port 8787
```

```bash
python3 scripts/setup_cron.py \
  --runner-url "https://YOUR_PUBLIC_RUNNER_URL" \
  --webhook-secret "$SAAS_SHORT_BOT_WEBHOOK_SECRET"
```

## Notes

- Use `paper-sim` first.
- Self-learning promotion requires gate checks; it does not auto-promote to live.
- Use `scripts/dry_run_prompt.txt` for a single copy/paste test run.
