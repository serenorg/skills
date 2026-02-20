---
name: ai-governance-assessment
description: "Assess AI governance maturity for compliance/risk leaders: evaluates 8 domains against NIST AI RMF, EU AI Act, ISO 42001, and OCC SR 11-7 frameworks, generating board-ready reports with gap analysis and prioritized roadmaps."
---

# AI Governance Readiness Assessment

Assess your organization's AI governance maturity using AI that never leaves your device.

## What This Skill Provides

- Structured intake questionnaire for organization context
- Maturity scoring across 8 governance domains (1-5 scale)
- Framework alignment (NIST AI RMF, EU AI Act, ISO 42001, OCC SR 11-7)
- Gap analysis with prioritized recommendations
- Board-ready assessment reports

## When to Use

Activate this skill when the user asks about:
- "AI governance assessment"
- "assess our AI readiness"
- "AI compliance check"
- "governance maturity evaluation"
- "NIST AI RMF assessment"
- "EU AI Act readiness"
- "ISO 42001 gap analysis"
- "AI risk assessment"

## Workflow

### Phase 1: Organization Context

Ask these intake questions:

1. **Industry**: Financial Services, Healthcare, Energy/Utilities, Government/Defense, Technology, Manufacturing, Other
2. **Size**: <100, 100-1000, 1000-10000, 10000+ employees
3. **AI Maturity**: Exploring (no production AI), Early (1-5 use cases), Scaling (5-20), Mature (20+)
4. **Regulatory Environment**: GDPR/EU AI Act, US Federal, Financial (OCC/SEC), Healthcare (HIPAA/FDA), Industry-specific
5. **Current State**: AI Center of Excellence, Ethics policy, Model inventory, Risk assessment process, Human oversight, Bias testing

### Phase 2: Framework Selection

Based on responses, determine applicable frameworks:

| Trigger | Framework |
|---------|-----------|
| EU operations | EU AI Act |
| US Federal/Government | NIST AI RMF |
| Financial services | OCC SR 11-7 |
| Healthcare | FDA AI/ML guidance |
| Any AI deployment | NIST AI RMF (baseline) |

### Phase 3: Domain Assessment

Evaluate maturity (1-5) across 8 domains:

1. **Strategy & Leadership** - Board oversight, AI governance body, executive sponsorship
2. **Risk Management** - AI risk inventory, assessment process, escalation procedures
3. **Data Governance** - Training data documentation, quality standards, consent management
4. **Model Lifecycle** - Versioning, approval workflows, drift detection, retirement
5. **Transparency** - Explainability, user disclosure, regulatory documentation
6. **Fairness & Bias** - Pre-deployment testing, protected attributes, remediation
7. **Security & Privacy** - AI-specific security, adversarial testing, privacy techniques
8. **Human Oversight** - High-risk review, override capability, operator training

**Scoring Guide:**
- 1: No governance, ad-hoc
- 2: Informal, reactive
- 3: Documented, emerging
- 4: Formal, proactive
- 5: Continuous, automated

### Phase 4: Gap Analysis

For each domain:
1. Calculate current score
2. Determine target based on risk profile
3. Identify gap (target - current)
4. Prioritize: CRITICAL (gap 3+, high urgency), HIGH, MEDIUM, LOW

### Phase 5: Generate Report

Output structured report with:

```markdown
# AI Governance Readiness Assessment
## [Organization Name] - [Date]

## Executive Summary
- Overall Score: X.X / 5.0
- Maturity Level: [Emerging/Developing/Established/Advanced/Leading]
- Key findings (3 bullets)
- Immediate actions (3 items)

## Maturity Scorecard
| Domain | Score | Target | Gap | Priority |
|--------|-------|--------|-----|----------|

## Framework Alignment
### [Framework Name]
Readiness: X%
| Requirement | Status | Gap |

## Detailed Findings
### Critical Gaps
For each: Current state, Target state, Risk, Recommendation, Effort, Timeline

## Recommended Roadmap
- Phase 1 (0-3 months): Foundation
- Phase 2 (3-6 months): Build
- Phase 3 (6-12 months): Scale

## Risk Register Template
| Risk ID | Category | Description | Likelihood | Impact | Controls | Actions |
```

## Example

**User**: "Can you assess our AI governance? We're a 2,000-employee regional bank with 3 AI systems."

**Agent workflow**:
1. Ask clarifying questions (AI maturity, regulations, current governance)
2. Determine frameworks (OCC SR 11-7, NIST AI RMF, Fair Lending)
3. Score each domain based on responses
4. Generate gap analysis with priorities
5. Output board-ready report with roadmap

See `examples/sample-assessment-regional-bank.md` for full output.

## Privacy Note

This skill is designed for secure, on-device AI:
- All assessment data stays local
- No cloud storage of compliance information
- Audit-ready methodology documentation

## Target Users

- Chief Compliance Officers
- AI/ML Governance Program Managers
- Risk Leaders in Regulated Industries
- Enterprise Architects evaluating AI adoption
