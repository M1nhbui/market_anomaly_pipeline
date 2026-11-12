# Step Functions orchestrates Silver → gate → Gold

The transform chain runs as an AWS Step Functions **Standard** state machine. It waits for the Silver Glue job to finish, invokes the gate Lambda, and reaches the Gold Glue job only through a Choice state on `quality_passed`. We chose it because the gate is useless unless it can actually stop Gold, and only an orchestrator that branches on a Lambda's answer can do that. At about 8 state transitions per run, its cost is negligible next to Glue's.

## Considered options

- **Glue Workflows**: Glue's native chaining. Its triggers fire on job state, not on a Lambda's return value, so a failed gate could not block Gold.
- **One Lambda that calls each step**: Lambda's 15-minute limit sits close to the measured chain duration (Silver 308 s plus Gold 133 s, before any retry), and a failure partway through leaves no visible history of what ran.

## Consequences

- **Retries:** Step Functions retries only transient errors (`Glue.ConcurrentRunsExceededException`, the cause of bug #16, and Lambda throttling or service errors), up to 3 attempts with backoff. A Glue job that fails or times out is not retried, because retrying a deterministic failure pays for the same crash again.
- **Alerts:** an SNS email is sent only on a blocking failure or a crashed step. Successes and warnings are not emailed; warnings are already persisted in `dq_results`. This drops the success notification the README originally proposed.

## Update, 2026-11-12: a second alert path for failures the chain can't catch

The code review of slice 8 found that "every failure emails" didn't hold. `States.ALL` doesn't catch `States.Runtime`, and timed-out or aborted executions never reach the chain's alert states, so those runs ended silently.

An EventBridge rule on the chain's "Execution Status Change" events now emails on runs that end TIMED_OUT, ABORTED, or FAILED with an error the chain didn't raise itself (ticket #9). It skips `GateBlocked` and `StepFailed`, which the chain already emailed about, and `InvalidLookback`, a rejected manual input that by design sends no email. A test keeps that skip-list equal to the chain's Fail errors.

Rejected:
- **Making the rule the only alert path.** One path is tidier, but it would mean rewriting ticket #3's alert states and tests, and losing the gate email's list of blocking checks.
- **Accepting duplicate emails.** A blocked gate would email twice, which trains the owner to skim alerts.
