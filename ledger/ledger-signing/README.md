# Ledger Signing

USB/HID runtime skill for signing transactions and messages on a Ledger device.

## Location

- Skill doc: `ledger/ledger-signing/SKILL.md`
- Runtime: `ledger/ledger-signing/scripts/agent.py`
- Human-in-the-loop tests: `ledger/ledger-signing/HITL_TESTS.md`

## Supported Payloads

- `transaction`
- `message`

Not yet implemented:

- `typed_data` (EIP-712)

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create config:

```bash
cp config.example.json config.json
```

3. Set runtime inputs in `config.json`:

- `dry_run`: `false`
- `inputs.payload_kind`
- `inputs.derivation_path`
- `inputs.payload_hex`

4. Run execute mode:

```bash
python scripts/agent.py --config config.json --execute
```

## Safety

- `dry_run=true` cannot sign.
- `--execute` is required for live device signing.
- The runtime uses direct Ledger USB/HID transport through `ledgerblue`.
