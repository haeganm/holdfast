"""Rebuild the AWS record from the sitting's exports in results/aws/inputs:

    python -I scripts/aws_report.py

writes results/aws/aws-run.json (the measured run: the harness's summary row,
the Lambda's own logs, CloudWatch, an estimated cost, both deploys and the
teardown check) and results/aws/first-run-cloudwatch.json (the set-aside first
run, whose Lambda logs were lost). The inputs came from the AWS CLI during the
sitting: `aws logs filter-log-events` for the log group, and `scripts/aws_check.sh`
for the metrics and the teardown check. Cost is an estimate at AWS's listed
prices (cloud/terraform/README.md), not a bill.
"""
import csv
import gzip
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AWS = ROOT / "results" / "aws"
IN = AWS / "inputs"


def total(metrics: dict, name: str, stat: str = "Sum") -> float:
    vals = [p[stat] for p in metrics[name]["Datapoints"]]
    return max(vals, default=0) if stat == "Maximum" else sum(vals)


def pct(xs: list, q: float):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def sitting(log: str) -> dict:
    """Apply, run and destroy times, and the tokenless check, from one deploy's run log."""
    text = (IN / log).read_text(encoding="utf-8")
    return {"applied": re.search(r"== apply (\S+)", text)[1], "run_started": re.search(r"== run (\S+)", text)[1],
            "destroy_started": re.search(r"== destroy (\S+)", text)[1],
            "tokenless_request_status": int(re.search(r"== a tokenless request must be refused\n(\d+)", text)[1])}


def teardown() -> dict:
    """What was left after each destroy, from the check's transcript; both checks must agree."""
    checks, cur = [], None
    for line in (IN / "teardown-check.txt").read_text(encoding="utf-8").splitlines():
        if line.startswith("== "):
            cur = {}
            checks.append(cur)
        elif cur is not None and ":" in line and not line.startswith("#"):
            key, _, rest = line.rpartition(":")
            cur[key.split()[0]] = rest.split()
    assert len(checks) == 2 and checks[0] == checks[1], "the two teardown checks should print the same"
    c = checks[0]
    return {"lambda_functions": c["lambda"], "dynamodb_tables": c["dynamodb"], "http_apis": c["api"],
            "iam_roles": c["iam"], "log_groups": c["logs"], "tagged_project_holdfast": c["tagged"],
            "budgets_kept": c["budgets"],
            "note": "names starting with holdfast left in each service, printed by scripts/aws_check.sh after each "
                    "destroy and transcribed in inputs/teardown-check.txt"}


def main() -> None:
    row = next(csv.DictReader(open(AWS / "summary.csv", newline="")))
    with gzip.open(IN / "lambda-logs.json.gz", "rt", encoding="utf-8") as f:
        events = json.load(f)["events"]
    metrics = json.loads((IN / "metrics.json").read_text(encoding="utf-8"))

    report_re = re.compile(r"Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+"
                           r"Max Memory Used: (\d+) MB(?:\s+Init Duration: ([\d.]+) ms)?")
    dur, billed, mem, init, requests = [], [], [], [], []
    for e in events:
        msg = e["message"]
        if msg.startswith("REPORT"):
            if m := report_re.search(msg):
                dur.append(float(m[1]))
                billed.append(int(m[2]))
                mem.append(int(m[4]))
                if m[5]:
                    init.append(float(m[5]))
        elif msg.startswith("{"):
            try:
                j = json.loads(msg)
            except ValueError:
                continue
            if j.get("msg") == "request":
                requests.append(j)

    by_status: dict[str, int] = {}
    for r in requests:
        key = f"{r.get('path')} {r.get('status')}"
        by_status[key] = by_status.get(key, 0) + 1
    gb_s = sum(billed) / 1000 * 0.125  # 128 MB
    rru = total(metrics, "ddb_ConsumedReadCapacityUnits")
    wru = total(metrics, "ddb_ConsumedWriteCapacityUnits")
    api_n = total(metrics, "api_Count")
    log_bytes = sum(len(e["message"].encode()) for e in events)
    access_line = 200  # bytes: more than the six short JSON fields of the stage's access log format take
    est = {"api_gateway": api_n / 1e6 * 1.00, "lambda_requests": len(dur) / 1e6 * 0.20,
           "lambda_duration_arm64": gb_s * 0.0000133334,
           "dynamodb_reads": rru / 1e6 * 0.125, "dynamodb_writes": wru / 1e6 * 0.625,
           "cloudwatch_logs_lambda": log_bytes / 2**30 * 0.50,
           "cloudwatch_logs_api_access_at_most": api_n * access_line / 2**30 * 0.50}
    out = {
        "harness": {k: row[k] for k in ("run_id", "settled", "settle_ms", "cloud_matches", "cloud_check_tries",
                                        "aircraft", "ingest_total", "prop_p50", "prop_p95", "prop_p99",
                                        "prop_unresolved", "stale_p50", "stale_p99", "sync_err_rate",
                                        "feed_delay_p50", "commit", "errors")},
        "lambda": {"invocations_logged": len(dur), "cold_starts": len(init), "init_ms": sorted(init),
                   "duration_ms_p50": pct(dur, .5), "duration_ms_p95": pct(dur, .95), "duration_ms_p99": pct(dur, .99),
                   "duration_ms_max": max(dur, default=None), "max_memory_mb": max(mem, default=None),
                   "billed_gb_s": round(gb_s, 2)},
        "peer": {"requests_by_path_status": by_status,
                 "table_items_max": max((r.get("stored", 0) for r in requests), default=0),
                 "writes": sum(r.get("changed", 0) for r in requests), "won": sum(r.get("won", 0) for r in requests),
                 "errors": sum(1 for r in requests if r.get("err"))},
        "cloudwatch": {"lambda_invocations": total(metrics, "lambda_Invocations"),
                       "lambda_errors": total(metrics, "lambda_Errors"),
                       "lambda_throttles": total(metrics, "lambda_Throttles"),
                       "lambda_max_concurrency": total(metrics, "lambda_ConcurrentExecutions", "Maximum"),
                       "api_requests": api_n, "api_4xx": total(metrics, "api_4xx"), "api_5xx": total(metrics, "api_5xx"),
                       "ddb_read_units": rru, "ddb_write_units": wru,
                       "ddb_read_throttles": total(metrics, "ddb_ReadThrottleEvents"),
                       "ddb_write_throttles": total(metrics, "ddb_WriteThrottleEvents"),
                       "window": metrics["window"]},
        "estimated_cost_usd": {k: round(v, 6) for k, v in est.items()} | {
            "total": round(sum(est.values()), 4),
            "note": f"listed us-east-1 prices with no free tier applied; Lambda logs are the {log_bytes} bytes of "
                    f"messages copied from the log group; the API's access logs were not measured and are counted "
                    f"at {access_line} bytes a line"},
        "deploys": {"set_aside": sitting("run-first.log"), "measured": sitting("run.log")},
        "teardown_check": teardown(),
    }
    (AWS / "aws-run.json").write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8", newline="\n")

    first = json.loads((IN / "metrics-first.json").read_text(encoding="utf-8"))
    summary: dict = {k: (total(first, k, "Maximum" if "Concurrent" in k else "Sum")) for k in first if k != "window"}
    summary["window"] = first["window"]
    summary["note"] = ("first deployed run, 2026-11-06 05:57-06:08 UTC: two of its polls timed out, so it was set "
                       "aside and run again; the Lambda logs were not kept (a copy step failed)")
    (AWS / "first-run-cloudwatch.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8",
                                                    newline="\n")
    print(f"wrote {AWS / 'aws-run.json'} and {AWS / 'first-run-cloudwatch.json'}")


if __name__ == "__main__":
    main()
