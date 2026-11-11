# Step Functions orchestrates Silver → gate → Gold

The transform chain runs as an AWS Step Functions **Standard** state machine. It waits for the Silver Glue job to finish, invokes the gate Lambda, and reaches the Gold Glue job only through a Choice state on `quality_passed`. We chose it because the gate is useless unless it can actually stop Gold, and only an orchestrator that branches on a Lambda's answer can do that. At about 8 state transitions per run, its cost is negligible next to Glue's.

## Considered options

- **Glue Workflows**: Glue's native chaining. Its triggers fire on job state, not on a Lambda's return value, so a failed gate could not block Gold.
- **One Lambda that calls each step**: Lambda's 15-minute limit sits close to the measured chain duration (Silver 308 s plus Gold 133 s, before any retry), and a failure partway through leaves no visible history of what ran.

## Consequences

- **Retries:** Step Functions retries only transient errors (`Glue.ConcurrentRunsExceededException`, the cause of bug #16, and Lambda throttling or service errors), up to 3 attempts with backoff. A Glue job that fails or times out is not retried, because retrying a deterministic failure pays for the same crash again.
- **Alerts:** an SNS email is sent only on a blocking failure or a crashed step. Successes and warnings are not emailed; warnings are already persisted in `dq_results`. This drops the success notification the README originally proposed.
