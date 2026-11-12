"""
Offline tests for the run-failed alert rule (ticket #9).

States.ALL does not catch States.Runtime, and timeouts or aborts never reach the
chain's own alert states. An EventBridge rule emails on those. It must SKIP failures
the chain already handled (it emailed, or it rejected a manual input on purpose), or
the owner gets two emails for one run. The skip-list therefore has to match the
chain's own Fail errors exactly, and these tests read both files to hold them in sync.

No AWS calls. The pattern itself is checked against AWS's matcher by hand with
`aws events test-event-pattern` (METRICS M-067).
"""

import json
from pathlib import Path

from test_state_machine import TEMPLATE_VARS, render

ROOT = Path(__file__).resolve().parent.parent
DEFINITION = ROOT / "state_machine" / "transform_chain.asl.json"
PATTERN = ROOT / "state_machine" / "run_failed_alert.pattern.json"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:test-chain"


def chain_fail_errors():
    sm = json.loads(render(DEFINITION.read_text(), TEMPLATE_VARS))
    return {s["Error"] for s in sm["States"].values() if s["Type"] == "Fail"}


def pattern():
    return json.loads(render(PATTERN.read_text(), {"state_machine_arn": STATE_MACHINE_ARN}))


def skipped_errors(p):
    """The error names the rule's FAILED branch refuses to email about."""
    (failed_branch,) = [b for b in p["detail"]["$or"] if b["status"] == ["FAILED"]]
    (anything_but,) = failed_branch["error"]
    return set(anything_but["anything-but"])


def test_rule_skips_exactly_the_failures_the_chain_already_handled():
    assert skipped_errors(pattern()) == chain_fail_errors()
