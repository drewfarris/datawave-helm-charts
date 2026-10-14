# Independently deployed annotation roles

Use `role: api` and `role: writer` in separate releases, with distinct `meta.name` values. Both releases keep the Spring application name `annotation` and share stable Rabbit destinations and consumer groups. The default role is `combined`. The examples in `examples/` set independent names and replica counts; writer ingress and annotation cache initialization are omitted.

```bash
helm upgrade --install annotation-api ./annotation -n datawave -f annotation/examples/api-values.yaml
helm upgrade --install annotation-writer ./annotation -n datawave -f annotation/examples/writer-values.yaml
```

The image must include annotation role support. Publish `configuration/configMapFiles/annotation-annotation-api.yml` and `annotation-annotation-writer.yml` to the system Config Server before deploying split roles. The chart prepends `annotation-api` or `annotation-writer` to `--spring.profiles.active` from its `role` value; each overlay sets `annotation.role`. Keep the role unset in shared `annotation.yml`. Enable writer backends in Config Server, provision their queues before admitting API traffic, and preserve group names when scaling or migrating. Each backend group receives its own copy; replicas within a group compete for messages. The API overlay disables direct fallback, so API success means broker-confirmed acceptance; reads can lag until a writer persists the message.

Both roles use the system's existing configuration and authorization services. The chart defaults point `CONFIG_SERVER_URL` at `configuration`; the shared application configuration supplies the authorization URI. Dedicated configuration or authorization releases are unnecessary.

Both releases use Spring application name `annotation`, so the profile `annotation-api` resolves to `annotation-annotation-api.yml`, and `annotation-writer` resolves to `annotation-annotation-writer.yml`. The repeated prefix follows Spring Cloud Config's application/profile filename convention. Role profiles load defaults before later environment or backend profiles, including the test overlays below. No Config Server code change is needed.

Remove legacy `ANNOTATION_ROLE` entries from Helm values; the chart rejects them. It also rejects conflicting role profiles and duplicate active-profile arguments. The service rejects both role profiles being active, a missing role overlay, or a profile whose effective `annotation.role` does not match. Without either role profile, the packaged default remains combined.

## Live validation

The opt-in `role-test` profile in `configuration/configMapFiles/annotation-role-test.yml` keeps test queues and tables separate while using those shared services. It disables direct file fallback so successful writes require real broker confirms. The shared mock authorization configuration gives the existing deployment-test server certificate `AnnotationWriter` using its existing `PUBLIC` authorization.

After the system Config Server contains this profile, add `annotation/examples/shared-system-test-values.yaml` after the API or writer values:

```bash
helm upgrade --install annotation-api ./annotation -n datawave \
  -f annotation/examples/api-values.yaml -f annotation/examples/shared-system-test-values.yaml
helm upgrade --install annotation-writer ./annotation -n datawave \
  -f annotation/examples/writer-values.yaml -f annotation/examples/shared-system-test-values.yaml
```

For a file-only writer, add `role-file` after `role-test` in the active profiles. `annotation-role-file.yml` selects the dump consumer and writes to `/logs/annotation-role-dump`; normal deployments keep their original file settings. These test profiles use fixed test queue/table names; use separate profiles or namespaces for concurrent independent test cohorts.

The standalone `../python_deployment_tests/annotation_roles_smoke.py` script tests separate annotation API and writer releases using only Python's standard library. Point the releases at isolated Rabbit destinations and Accumulo tables, with the API's direct file fallback disabled. Forward each API pod separately to verify each replica; forwarding a Service selects just one pod.

```bash
python3 python_deployment_tests/annotation_roles_smoke.py \
  --api-url https://localhost:18643/annotation \
  --api-url https://localhost:18645/annotation \
  --writer-url https://localhost:18644/annotation \
  --cert python_deployment_tests/resources/test.crt.pem --key python_deployment_tests/resources/test.key.pem --insecure \
  --payload /path/to/annotation.json --count 12 --output /tmp/annotation-role-smoke.json
```

The certificate identity needs annotation write authorization. The script checks health and the writer's missing REST endpoint, submits concurrent unique annotations, and verifies segments and sources through every API replica. It leaves its test annotations in the configured tables. Queue depth, consumer counts, and writer-down backlog recovery can be checked separately while scaling only the isolated test deployments. Production file writers need persistent storage; the chart's default `/logs` volume is an `emptyDir`.

