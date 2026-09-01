// Silence dependency log output during tests.
//
// db-federation aliases in sibling packages (@agentix-e/db-cluster and the
// db-operations chaosengine). Those packages emit Raft election / CDC / chaos
// pino logs at `info` level when their code is exercised under test, which
// floods stdout and slows down the run. Setting `LOG_LEVEL=silent` (read by
// the shared `LoggingAdapter`) and `VITEST=true` keeps unit-test output
// deterministic and readable, mirroring the db-cluster test setup.
process.env.LOG_LEVEL = 'silent';
process.env.VITEST = 'true';
