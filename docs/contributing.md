# Contributing

Contributions are welcome! The plugin is a small codebase (~1k LOC of
Groovy) with an intentionally small surface area, so first contributions
are usually approachable.

## Code layout

```
src/main/groovy/nextflow/executor/
├── BacalhauExecutor.groovy        # Executor lifecycle + job translation
└── BacalhauTaskHandler.groovy     # Per-task submit / check / retrieve

src/main/resources/META-INF/
└── extensions.idx                 # Plugin service registration

src/test/groovy/
├── BacalhauExecutorTest.groovy    # Unit tests
└── BacalhauIntegrationTest.groovy # Integration tests
```

## Build & test

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)    # macOS
./gradlew build                 # compile + unit tests
./gradlew test                  # unit tests only
./gradlew integrationTest       # requires a running Bacalhau node
./gradlew publishToMavenLocal   # publish 0.1.0-SNAPSHOT to ~/.m2
```

## Pull-request checklist

- [ ] Tests updated or added for the change.
- [ ] `./gradlew build` passes.
- [ ] The federated-af demo still runs end-to-end
      (`./examples/federated-af/run.sh --skip-bacalhau`).
- [ ] Commit message uses Conventional Commits (`fix:`, `feat:`, `docs:`, …).
- [ ] Docs under `docs/` updated if behavior or configuration changed.

## Reporting bugs

Include:

- Nextflow version (`nextflow -version`)
- Bacalhau version (`bacalhau version`)
- Plugin commit SHA
- A minimal `.nf` + `nextflow.config` that reproduces the issue
- Relevant output of `nextflow run ... -with-trace -with-report`

Bug reports with a failing test case are the fastest to land.

## License

Contributions are under the Apache License 2.0 (same as the project).
