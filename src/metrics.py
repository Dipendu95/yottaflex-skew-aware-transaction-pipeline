import json
from datetime import datetime, timezone

from src.config import LOG_DIR


def save_run_metrics(
    run_id,
    source_file,
    input_count,
    structurally_valid_count,
    structural_rejected_count,
    accepted_count,
    business_rejected_count,
    start_time,
    run_status="SUCCESS",
    failure_reason=None
):
    end_time = datetime.now(timezone.utc)

    accounted_count = (
        accepted_count
        + business_rejected_count
        + structural_rejected_count
    )

    reconciliation_passed = input_count == accounted_count

    if not reconciliation_passed:
        run_status = "FAILED"
        failure_reason = (
            f"Reconciliation failed: input={input_count}, "
            f"accounted={accounted_count}"
        )

    metrics = {
        "run_id": run_id,
        "source_file": source_file,
        "run_status": run_status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "input_count": input_count,
        "structurally_valid_count": structurally_valid_count,
        "structural_rejected_count": structural_rejected_count,
        "accepted_count": accepted_count,
        "business_rejected_count": business_rejected_count,
        "total_rejected_count": (
            structural_rejected_count + business_rejected_count
        ),
        "accounted_count": accounted_count,
        "reconciliation_passed": reconciliation_passed,
        "failure_reason": failure_reason
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = LOG_DIR / f"{run_id}_metrics.json"

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4)

    return metrics
