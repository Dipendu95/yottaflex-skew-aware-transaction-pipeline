import csv
from pathlib import Path


def preflight_csv(file_path):
    """
    Checks CSV structure before Spark ingestion.

    Valid records are written to a clean temporary CSV.
    Structurally invalid records are returned separately.

    Assumption: one physical line per CSV record.
    """

    file_path = Path(file_path)

    clean_file = (
        file_path.parent /
        f"{file_path.stem}_preflight.csv"
    )

    structural_rejects = []
    valid_count = 0

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as source, open(
        clean_file,
        "w",
        encoding="utf-8",
        newline=""
    ) as clean:

        reader = csv.reader(source, strict=True)
        writer = csv.writer(clean)

        header = next(reader)
        expected_fields = len(header)

        writer.writerow(header)

        for line_number, raw_line in enumerate(source, start=2):
            try:
                parsed = next(
                    csv.reader([raw_line], strict=True)
                )

                if len(parsed) != expected_fields:
                    raise ValueError(
                        f"EXPECTED_{expected_fields}_FIELDS_"
                        f"FOUND_{len(parsed)}"
                    )

                writer.writerow(parsed)
                valid_count += 1

            except (csv.Error, ValueError) as error:
                structural_rejects.append({
                    "source_file": file_path.name,
                    "source_line_number": line_number,
                    "raw_record": raw_line.rstrip("\r\n"),
                    "reject_reason": (
                        f"STRUCTURAL_ERROR: {error}"
                    )
                })

    return clean_file, structural_rejects, valid_count
