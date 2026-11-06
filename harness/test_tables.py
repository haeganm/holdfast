"""README.md's results tables against the ones tables.py builds from the
committed summary.csv files, so the README can't drift from the data.

    python -m unittest discover -s harness
"""

import unittest
from pathlib import Path

import tables

ROOT = Path(__file__).resolve().parent.parent


def blocks(lines: list[str]) -> list[list[str]]:
    """Each run of consecutive table lines."""
    out: list[list[str]] = []
    prev = ""
    for line in lines:
        if line.startswith("|"):
            if prev.startswith("|"):
                out[-1].append(line)
            else:
                out.append([line])
        prev = line
    return out


class ReadmeTablesTest(unittest.TestCase):
    def test_every_results_table_in_the_readme_is_the_data_verbatim(self):
        r = ROOT / "results"
        built = blocks(tables.report(r / "sweep-sync", r / "sweep-clock", r / "real-hour", r / "real-base", r / "cloud",
                                     r / "star-control", r / "live"))
        self.assertEqual(len(built), 7)
        readme = {b[0]: b for b in blocks((ROOT / "README.md").read_text(encoding="utf-8").splitlines())}
        for table in built:
            with self.subTest(table[0]):
                self.assertEqual(readme.get(table[0]), table)


if __name__ == "__main__":
    unittest.main()
