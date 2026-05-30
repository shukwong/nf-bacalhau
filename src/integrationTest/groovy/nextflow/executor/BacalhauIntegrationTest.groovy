/*
 * Copyright 2024, nf-bacalhau contributors
 * Licensed under the Apache License, Version 2.0
 */
package nextflow.executor

import spock.lang.Requires
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Live Bacalhau smoke checks.
 *
 * These tests are opt-in because they require a Bacalhau CLI and a reachable
 * orchestrator. Run with BACALHAU_INTEGRATION=1 ./gradlew integrationTest.
 */
@Requires({ env.BACALHAU_INTEGRATION == '1' })
class BacalhauIntegrationTest extends Specification {

    def 'bacalhau cli is available'() {
        when:
        def proc = new ProcessBuilder('bacalhau', 'version')
            .redirectErrorStream(true)
            .start()
        def finished = proc.waitFor(30, TimeUnit.SECONDS)

        then:
        finished
        proc.exitValue() == 0
    }
}
