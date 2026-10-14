#!/usr/bin/env python3
"""Exercise independently deployed annotation roles against a live broker and store."""

import argparse
import copy
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", action="append", required=True, help="Include /annotation; repeat for individual API replicas")
    parser.add_argument("--writer-url", required=True, help="Writer URL including /annotation")
    parser.add_argument("--cert", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--ca", type=Path)
    parser.add_argument("--insecure", action="store_true", help="Allow local development certificates")
    parser.add_argument("--payload", type=Path, required=True, help="Valid annotation JSON; use isolated destination/tables")
    parser.add_argument("--auths", default="PUBLIC,PRIVATE")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=60, help="Seconds to await asynchronous persistence")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.count < 1 or args.timeout <= 0:
        parser.error("count and timeout must be positive")
    tls = ssl.create_default_context(cafile=str(args.ca) if args.ca else None)
    if args.insecure:
        tls.check_hostname = False
        tls.verify_mode = ssl.CERT_NONE
    tls.load_cert_chain(args.cert, args.key)
    payload = json.loads(args.payload.read_text())
    document = ":".join(payload[field] for field in ("shard", "dataType", "uid"))
    path = "/v1/DOCUMENT/" + urllib.parse.quote(document, safe=":")
    query = "?" + urllib.parse.urlencode({"auths": args.auths})
    run_id = uuid.uuid4().hex
    results = {"run_id": run_id, "api_urls": args.api_url, "writer_url": args.writer_url, "count": args.count, "checks": [], "status": "running"}

    def request(base, suffix, method="GET", body=None):
        req = urllib.request.Request(base.rstrip("/") + suffix, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json", "Accept": "application/json"}, method=method)
        try:
            response = urllib.request.urlopen(req, context=tls, timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = raw.decode(errors="replace")
            return response.status, body

    def check(name, condition):
        results["checks"].append({"name": name, "passed": bool(condition)})
        if not condition:
            raise AssertionError(name)

    def write(index):
        annotation = copy.deepcopy(payload)
        annotation.pop("annotationId", None)
        annotation["annotationType"] = f"annotation-role-smoke-{run_id}-{index}"
        annotation.setdefault("metadata", {})["role_test_run"] = run_id
        annotation["source"]["model"] = f"role-smoke-{run_id}"
        annotation["source"].setdefault("metadata", {})["role_test_run"] = run_id
        for segment in annotation.get("segments", []):
            segment.pop("segmentHash", None)
        base = args.api_url[index % len(args.api_url)]
        status, accepted = request(base, path + "/annotation" + query, "POST", annotation)
        if status != 200 or not isinstance(accepted, dict) or not accepted.get("annotationId"):
            raise AssertionError(f"write {index} through {base}: HTTP {status}")
        return accepted

    def persisted_source(source):
        # The API masks source visibility when injecting a source into an annotation.
        normalized = copy.deepcopy(source)
        normalized.get("metadata", {}).pop("visibility", None)
        return normalized

    try:
        for base in args.api_url + [args.writer_url]:
            status, health = request(base, "/mgmt/health")
            check(f"{base} is healthy", status == 200 and health["status"] == "UP")
        status, _ = request(args.writer_url, path + query)
        check("writer has no annotation REST endpoint", status == 404)
        with ThreadPoolExecutor(max_workers=min(args.count, 6)) as pool:
            accepted = list(pool.map(write, range(args.count)))
        check("all API writes were producer-confirmed", len(accepted) == args.count)
        results["annotation_ids"] = [a["annotationId"] for a in accepted]
        for base in args.api_url:
            for expected in accepted:
                deadline = time.monotonic() + args.timeout
                while True:
                    status, stored = request(base, path + "/annotation/" + expected["annotationId"] + query)
                    if status == 200:
                        break
                    if status != 404 or time.monotonic() >= deadline:
                        raise AssertionError(f"persistence of {expected['annotationId']} through {base}: HTTP {status}")
                    time.sleep(0.5)
                check(f"{base} returns one annotation {expected['annotationId']}", isinstance(stored, list) and len(stored) == 1)
                stored = stored[0]
                check(f"{base} reads complete annotation {expected['annotationId']}",
                      stored["annotationType"] == expected["annotationType"] and stored["segments"] == expected["segments"]
                      and stored["metadata"]["role_test_run"] == run_id and stored["source"] == persisted_source(expected["source"]))
                status, source = request(base, "/v1/source/" + expected["source"]["analyticSourceHash"] + query)
                check(f"{base} reads source for {expected['annotationId']}", status == 200 and source == expected["source"])
        results["status"] = "passed"
    except Exception as error:
        results["status"] = "failed"
        results["error"] = str(error)
        raise
    finally:
        args.output.write_text(json.dumps(results, indent=2) + "\n")
        print(f"{results['status']}: {len(results['checks'])} checks; evidence {args.output}")


if __name__ == "__main__":
    main()
