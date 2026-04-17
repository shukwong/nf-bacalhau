# Status & Roadmap

## Completed (Phases 1–3)

- [x] Core executor infrastructure (`AbstractGridExecutor` extension)
- [x] Job submission and monitoring via Bacalhau CLI
- [x] Docker container support
- [x] Script and input file staging
- [x] Output file retrieval and verification
- [x] Resource management: CPU, memory, disk, time, GPU
- [x] Native S3 input support
- [x] Host-path input support (`host://` URIs)
- [x] Secret injection via `ext.bacalhauSecrets`
- [x] Comprehensive error handling with timeouts
- [x] Configuration validation and loading
- [x] Thread-safe synchronization in queue-status cache
- [x] Strict job-ID validation
- [x] JSON-based queue-status parsing with pagination support
- [x] Input validation and security hardening

## In progress (Phase 4)

- [ ] Performance tuning and optimization
- [ ] Extensive integration testing against a multi-node Bacalhau cluster
- [ ] Advanced networking configuration
- [ ] Benchmarking against Slurm / AWS Batch / Kubernetes for the genomics
      reference workload
- [ ] Publication of the plugin to the Nextflow plugin registry
- [ ] Expanded documentation and end-to-end examples

## Known limitations

- **Plugin is built locally, not registry-hosted.** Until publication, users
  must `./gradlew assemble` and stage the JAR manually (or via the bundled
  runner scripts).
- **Nextflow 23.10.x only.** Newer Nextflow versions change internal APIs;
  a 25.x port is on the roadmap.
- **CLI dependency.** The executor shells out to the Bacalhau CLI; future
  work will evaluate a native client library when one becomes available.
- **Queue status is cached, not event-driven.** Large workflows (thousands
  of concurrent tasks) will spend noticeable wall-time in the 5-second poll
  cycle. A subscribe-based model is a future optimization.

## Where to get help

- **GitHub Issues**: [shukwong/nextflow_on_bacalhau/issues](https://github.com/shukwong/nextflow_on_bacalhau/issues)
- **Bacalhau docs**: [docs.bacalhau.org](https://docs.bacalhau.org)
- **Nextflow community**: [community.nextflow.io](https://community.nextflow.io)
