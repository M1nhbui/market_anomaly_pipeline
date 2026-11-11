"""
Offline tests for the transform chain's state machine definition (ADR-0001).

The definition is a map: states are places, and Next / Default / Choices / Catch
are roads between them. These tests walk the roads and assert the PROMISES the
design makes (Gold only after a passed gate, Glue steps that wait for their job),
not the layout of the JSON. They identify the Silver job, the Gold job and the gate
by what they call, never by state name, so renaming or reordering states does not
break them. Only a broken promise does.

No AWS calls. The definition is a Terraform template, so it is rendered here with
placeholder values, the same substitution Terraform's templatefile() performs.
"""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DEFINITION = ROOT / "state_machine" / "transform_chain.asl.json"

TEMPLATE_VARS = {
    "silver_job_name": "test-bronze-to-silver",
    "gold_job_name": "test-silver-to-gold",
    "gate_function_arn": "arn:aws:lambda:us-east-1:000000000000:function:test-dq-gate",
    "alert_topic_arn": "arn:aws:sns:us-east-1:000000000000:test-alerts",
}

GLUE_RUN_AND_WAIT = "arn:aws:states:::glue:startJobRun.sync"
LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
SNS_PUBLISH = "arn:aws:states:::sns:publish"

# The only errors worth retrying: AWS bookkeeping that succeeds on a second try.
# A Glue job that genuinely FAILED, or a gate that raised, is deterministic - a retry
# pays for the same crash again (ADR-0001).
TRANSIENT_GLUE = {"Glue.ConcurrentRunsExceededException"}
TRANSIENT_LAMBDA = {
    "Lambda.TooManyRequestsException",
    "Lambda.ServiceException",
    "Lambda.AWSLambdaException",
    "Lambda.SdkClientException",
}
MAX_ATTEMPTS = 3


def render(text, variables):
    """templatefile()'s substitution: ${name} -> value. Unknown names are an error."""

    def sub(match):
        name = match.group(1)
        if name not in variables:
            raise KeyError(f"template references unknown variable ${{{name}}}")
        return variables[name]

    return re.sub(r"\$\{(\w+)\}", sub, text)


@pytest.fixture(scope="module")
def sm():
    return json.loads(render(DEFINITION.read_text(), TEMPLATE_VARS))


# --- graph helpers ----------------------------------------------------------------


def roads_from(state):
    """Every (target, how) road leaving a state."""
    roads = []
    if "Next" in state:
        roads.append((state["Next"], "next"))
    if "Default" in state:
        roads.append((state["Default"], "default"))
    for rule in state.get("Choices", []):
        roads.append((rule["Next"], ("choice", json.dumps(rule, sort_keys=True))))
    for catcher in state.get("Catch", []):
        roads.append((catcher["Next"], "catch"))
    return roads


def reachable(sm, start=None, without=()):
    start = start or sm["StartAt"]
    if start in without:
        return set()
    seen, todo = {start}, [start]
    while todo:
        for target, _ in roads_from(sm["States"][todo.pop()]):
            if target not in seen and target not in without:
                seen.add(target)
                todo.append(target)
    return seen


def glue_state(sm, job_name):
    found = [
        name for name, s in sm["States"].items()
        if s.get("Parameters", {}).get("JobName") == job_name
    ]
    assert len(found) == 1, f"expected exactly one state running {job_name}, found {found}"
    return found[0]


def gate_state(sm):
    found = [
        name for name, s in sm["States"].items()
        if s.get("Parameters", {}).get("FunctionName") == TEMPLATE_VARS["gate_function_arn"]
    ]
    assert len(found) == 1, f"expected exactly one state invoking the gate, found {found}"
    return found[0]


# --- the definition is well-formed --------------------------------------------------


def test_every_template_variable_is_supplied_and_substituted():
    text = render(DEFINITION.read_text(), TEMPLATE_VARS)
    assert "${" not in text
    json.loads(text)


def test_every_road_leads_to_a_real_state(sm):
    for name, state in sm["States"].items():
        for target, _ in roads_from(state):
            assert target in sm["States"], f"{name} points at missing state {target}"


def test_every_state_is_reachable_from_the_start(sm):
    assert reachable(sm) == set(sm["States"])


# --- the promises -------------------------------------------------------------------


def test_gold_is_entered_only_from_the_gate_choice_on_a_pass(sm):
    gold = glue_state(sm, TEMPLATE_VARS["gold_job_name"])
    roads_in = [
        (name, how)
        for name, state in sm["States"].items()
        for target, how in roads_from(state)
        if target == gold
    ]
    assert len(roads_in) == 1, f"Gold must have exactly one way in, found {roads_in}"
    source, how = roads_in[0]
    assert sm["States"][source]["Type"] == "Choice"
    assert how[0] == "choice", "Gold must be reached by an explicit rule, never the Default"
    rule = json.loads(how[1])
    assert rule.get("BooleanEquals") is True
    assert rule["Variable"].endswith(".quality_passed")


def test_the_choice_reads_the_answer_the_gate_actually_returned(sm):
    gate = sm["States"][gate_state(sm)]
    gold = glue_state(sm, TEMPLATE_VARS["gold_job_name"])
    choice = next(
        s for s in sm["States"].values()
        if s["Type"] == "Choice" and any(r["Next"] == gold for r in s["Choices"])
    )
    # The gate's Lambda response arrives under $.Payload; the task keeps the three
    # fields the chain needs and stores them at ResultPath. The Choice must read
    # quality_passed from exactly there, or it would test a field that never exists.
    assert gate["ResultSelector"]["quality_passed.$"] == "$.Payload.quality_passed"
    rule = next(r for r in choice["Choices"] if r["Next"] == gold)
    assert rule["Variable"] == gate["ResultPath"] + ".quality_passed"


def test_gold_cannot_be_reached_without_running_silver_and_the_gate(sm):
    gold = glue_state(sm, TEMPLATE_VARS["gold_job_name"])
    silver = glue_state(sm, TEMPLATE_VARS["silver_job_name"])
    assert gold not in reachable(sm, without={silver})
    assert gold not in reachable(sm, without={gate_state(sm)})


def test_silver_runs_before_the_gate(sm):
    silver = glue_state(sm, TEMPLATE_VARS["silver_job_name"])
    assert sm["StartAt"] == silver
    assert gate_state(sm) in reachable(sm, start=silver)


def test_glue_steps_wait_for_their_job_to_finish(sm):
    # Without .sync, Step Functions only STARTS the job and moves on, so the gate
    # would check Silver while Silver was still being rewritten.
    for job in ("silver_job_name", "gold_job_name"):
        state = sm["States"][glue_state(sm, TEMPLATE_VARS[job])]
        assert state["Resource"] == GLUE_RUN_AND_WAIT


def test_a_blocked_gate_ends_the_run_as_failed_without_gold(sm):
    gold = glue_state(sm, TEMPLATE_VARS["gold_job_name"])
    choice = next(
        s for s in sm["States"].values()
        if s["Type"] == "Choice" and any(r["Next"] == gold for r in s["Choices"])
    )
    after_block = reachable(sm, start=choice["Default"])
    assert gold not in after_block
    ends = [n for n in after_block if sm["States"][n]["Type"] in ("Fail", "Succeed") or sm["States"][n].get("End")]
    assert ends and all(sm["States"][n]["Type"] == "Fail" for n in ends), \
        "a run the gate blocked must show as failed in the console, not succeeded"
    assert any(is_alert(sm["States"][n]) for n in after_block), \
        "a run the gate blocked must email before it fails"


# --- alerts and retries (ticket #3) -------------------------------------------------


def is_alert(state):
    return state.get("Resource") == SNS_PUBLISH


def work_states(sm):
    """Task states that do pipeline work: the two Glue jobs and the gate."""
    return [n for n, s in sm["States"].items() if s["Type"] == "Task" and not is_alert(s)]


def happy_roads(state):
    """Roads a run takes when nothing goes wrong: Next, and Choice rules (not Default)."""
    roads = [state["Next"]] if "Next" in state else []
    roads += [r["Next"] for r in state.get("Choices", [])]
    return roads


def test_every_work_step_catches_every_error(sm):
    for name in work_states(sm):
        catchers = sm["States"][name].get("Catch", [])
        assert any("States.ALL" in c["ErrorEquals"] for c in catchers), \
            f"{name} has no catch-all: a crash there would end the run with no email"


def test_every_failure_road_emails_then_fails_without_reaching_gold(sm):
    gold = glue_state(sm, TEMPLATE_VARS["gold_job_name"])
    for name in work_states(sm):
        for catcher in sm["States"][name].get("Catch", []):
            after = reachable(sm, start=catcher["Next"])
            assert gold not in after, f"a failure in {name} must never lead to Gold"
            assert any(is_alert(sm["States"][n]) for n in after), f"a failure in {name} must email"
            ends = [n for n in after if sm["States"][n]["Type"] in ("Fail", "Succeed") or sm["States"][n].get("End")]
            assert ends and all(sm["States"][n]["Type"] == "Fail" for n in ends), \
                f"a failure in {name} must end the run as failed"


def test_a_successful_run_sends_no_email(sm):
    seen, todo = {sm["StartAt"]}, [sm["StartAt"]]
    while todo:
        for target in happy_roads(sm["States"][todo.pop()]):
            if target not in seen:
                seen.add(target)
                todo.append(target)
    assert not [n for n in seen if is_alert(sm["States"][n])], \
        "success and warnings must not email (ADR-0001)"


def test_only_transient_errors_are_retried_and_at_most_three_times(sm):
    for name in work_states(sm):
        state = sm["States"][name]
        allowed = TRANSIENT_GLUE if state["Resource"] == GLUE_RUN_AND_WAIT else TRANSIENT_LAMBDA
        for retrier in state.get("Retry", []):
            assert set(retrier["ErrorEquals"]) <= allowed, \
                f"{name} retries {retrier['ErrorEquals']}; only {sorted(allowed)} may be retried"
            assert retrier["MaxAttempts"] <= MAX_ATTEMPTS
            assert retrier.get("BackoffRate", 1.0) > 1.0, "retries should back off"


def test_the_transient_errors_actually_are_retried(sm):
    # The other half of the rule: bug #16 recurred on 2026-11-11 (M-061). A schedule
    # that starts right after any manual Glue run must ride it out, not email.
    for name in work_states(sm):
        state = sm["States"][name]
        wanted = TRANSIENT_GLUE if state["Resource"] == GLUE_RUN_AND_WAIT else TRANSIENT_LAMBDA
        retried = {e for r in state.get("Retry", []) for e in r["ErrorEquals"]}
        assert wanted <= retried, f"{name} does not retry {sorted(wanted - retried)}"


def test_alerts_publish_to_the_configured_topic(sm):
    alerts = [s for s in sm["States"].values() if is_alert(s)]
    assert alerts
    for a in alerts:
        assert a["Parameters"]["TopicArn"] == TEMPLATE_VARS["alert_topic_arn"]
