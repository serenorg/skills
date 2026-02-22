# Spectra MCP Tools Used By This Skill

This skill assumes the `mcp-spectra` publisher for the Spectra MCP server from:
`https://github.com/Finanzgoblin/spectra-mcp-server`

Core tools used:
- `scan_opportunities`: capital-aware PT ranking by chain and size
- `quote_trade`: PT trade quote with impact/slippage context
- `simulate_portfolio_after_trade`: before/after portfolio deltas
- `get_looping_strategy` (optional): PT + Morpho looping context

Utility tools commonly paired during analysis:
- `get_supported_chains`
- `get_pt_details`
- `compare_yield`
- `get_morpho_markets`

Constraint:
- Spectra MCP is read-only. It does not sign or broadcast transactions.
- Execution must be handled by an external signer/executor.
