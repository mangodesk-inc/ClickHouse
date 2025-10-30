#!/usr/bin/env python3
import argparse
import ast
import io
import subprocess
import sys
import urllib.request
from pathlib import Path

try:
    from unidiff import PatchSet
except Exception as exc:
    print(
        "Missing dependency 'unidiff' (pip install unidiff): {}".format(exc),
        file=sys.stderr,
    )
    sys.exit(2)


class DiffToSymbols:
    def __init__(self, clickhouse_path: str, pr_number: int):
        if Path(clickhouse_path).is_dir():
            self.clickhouse_path = clickhouse_path + "/clickhouse"
        else:
            self.clickhouse_path = clickhouse_path
        # TODO: add support for non api mode (from git)
        self.pr_number = pr_number
        assert self.pr_number > 0, "Works only for PRs"
        assert Path(
            self.clickhouse_path
        ).is_file(), f"clickhouse binary not found at {self.clickhouse_path}"

    @staticmethod
    def fetch(url: str) -> bytes:
        with urllib.request.urlopen(url) as resp:
            return resp.read()

    @staticmethod
    def parse_diff_to_csv(diff_bytes: bytes) -> str:
        patch = PatchSet(diff_bytes.decode("utf-8", errors="ignore"))
        out = io.StringIO()
        out.write("filename,line\n")
        exts = (".cpp", ".cc", ".cxx", ".c", ".hpp", ".hh", ".hxx", ".h", ".ipp")
        for f in patch:
            if not f.path.endswith(exts):
                continue
            for hunk in f:
                for line in hunk:
                    if line.is_added:
                        out.write("{},{}\n".format(f.path, line.target_line_no))
        return out.getvalue()

    def run_query(self, csv_payload: str) -> str:
        query = (
            """
        SELECT
            groupUniqArray(empty(linkage_name)
                ? demangle(addressToSymbol(address))
                : demangle(linkage_name) AS symbol)
        FROM file('stdin', 'CSVWithNames', 'filename String, line UInt32') AS diff
        ASOF JOIN
        (
            SELECT
                decl_file,
                decl_line,
                linkage_name,
                ranges[1].1 AS address
            FROM file('{ch_path}', 'DWARF')
            WHERE (tag = 'subprogram') AND (notEmpty(linkage_name) OR address != 0) AND notEmpty(decl_file)
        ) AS binary
        ON basename(diff.filename) = basename(binary.decl_file) AND diff.line >= binary.decl_line
        FORMAT TSV
            """.format(
                ch_path=self.clickhouse_path
            )
        ).strip()

        proc = subprocess.run(
            [self.clickhouse_path, "local", "--query", query],
            input=csv_payload,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            print(proc.stderr, file=sys.stderr)
            raise SystemExit(proc.returncode)
        return proc.stdout

    def get_symbols(self):
        diff_url = f"https://patch-diff.githubusercontent.com/raw/ClickHouse/ClickHouse/pull/{self.pr_number}.diff"
        diff_bytes = self.fetch(diff_url)
        csv_payload = self.parse_diff_to_csv(diff_bytes)
        symbols_str = self.run_query(csv_payload)
        # Convert ClickHouse TSV array format to Python list
        symbols = ast.literal_eval(symbols_str.strip())
        return symbols


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List changed symbols for a PR by parsing diff and querying ClickHouse."
    )
    parser.add_argument("pr", help="PR number")
    parser.add_argument(
        "clickhouse_path",
        help='Path to clickhouse binary (will be executed as "clickhouse local")',
    )
    args = parser.parse_args()
    output = DiffToSymbols(args.clickhouse_path, int(args.pr)).get_symbols()
    print(output)
