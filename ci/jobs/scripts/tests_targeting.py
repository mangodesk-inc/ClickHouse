import ast
import os
import re
import sys
from pathlib import Path

sys.path.append("./")

from ci.jobs.scripts.pr_diff_to_symbols import DiffToSymbols
from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Query to fetch failed tests from CIDB for a given PR.
# Only returns tests from commit_sha/check_name combinations that have less than 10 failures.
# This helps filter out commits with widespread test failures
FAILED_TESTS_QUERY = """ \
 select distinct test_name
 from (
          select test_name, commit_sha, check_name
          from checks
          where 1
            and pull_request_number = {PR_NUMBER}
            and check_name LIKE 'Stateless%'
            and check_status = 'failure'
            and match(test_name, '^[0-9]{{5}}_')
            and test_status = 'FAIL'
            and check_start_time >= now() - interval 300 day
          order by check_start_time desc
              limit 10000
      )
 where (commit_sha, check_name) IN (
     select commit_sha, check_name
     from checks
     where 1
       and pull_request_number = {PR_NUMBER}
   and check_name LIKE 'Stateless%'
   and check_status = 'failure'
   and test_status = 'FAIL'
   and check_start_time >= now() - interval 300 day
 group by commit_sha, check_name
 having count(test_name) < 20
     ) \
"""


class Targeting:

    def __init__(self, info: Info):
        assert info.pr_number > 0, "Targeting is applicable only for PRs"
        self.info = info

    def get_changed_tests(self):
        # TODO: add support for integration tests
        result = set()
        if self.info.is_local_run:
            changed_files = Shell.get_output(
                "git diff --name-only $(git merge-base master HEAD)"
            ).splitlines()
        else:
            changed_files = self.info.get_changed_files()
        assert changed_files, "No changed files"

        for fpath in changed_files:
            if re.match(r"tests/queries/0_stateless/\d{5}", fpath):
                if not Path(fpath).exists():
                    print(f"File '{fpath}' was removed — skipping")
                    continue

                print(f"Detected changed test file: '{fpath}'")

                fname = os.path.basename(fpath)
                fname_without_ext = os.path.splitext(fname)[0]

                # Add '.' suffix to precisely match this test only
                result.add(f"{fname_without_ext}.")

            elif fpath.startswith("tests/queries/"):
                # Log any other suspicious file in tests/queries for future debugging
                print(
                    f"File '{fpath}' changed, but doesn't match expected test pattern"
                )

        return sorted(result)

    def get_previously_failed_tests(self):
        from ci.praktika.cidb import CIDB
        from ci.praktika.settings import Settings

        tests = []
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        query = FAILED_TESTS_QUERY.format(PR_NUMBER=self.info.pr_number)
        query_result = cidb.query(query, log_level="")
        # Parse test names from the query result
        for line in query_result.strip().split("\n"):
            if line.strip():
                # Split by whitespace and get the first column (test_name)
                parts = line.split()
                if parts:
                    test_name = parts[0]
                    tests.append(test_name)
        print(f"Parsed {len(tests)} test names: {tests}")
        tests = list(set(tests))
        return sorted(tests)

    def get_changed_symbols(self, path):
        return DiffToSymbols(path, self.info.pr_number).get_symbols()

    def get_tests_by_changed_symbols(self, symbols):
        """
        returns mapping of symbol to array of tests that cover it
        """
        SYMBOL_TO_TESTS_QUERY = """
        SELECT groupArray(test_name) as tests
        from checks_coverage_inverted
        where 1
        and check_start_time > now() - interval 3 days
        and check_name LIKE 'Stateless%'
        and symbol = '{}'
        """
        symbol_to_tests = {}
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        for symbol in symbols:
            query = SYMBOL_TO_TESTS_QUERY.format(symbol)
            result = cidb.query(query, log_level="")
            # Parse the ClickHouse array result
            if result.strip():
                try:
                    tests = ast.literal_eval(result.strip())
                    symbol_to_tests[symbol] = tests if isinstance(tests, list) else []
                except (ValueError, SyntaxError):
                    print(f"Failed to parse tests for symbol '{symbol}': {result}")
                    symbol_to_tests[symbol] = []
            else:
                symbol_to_tests[symbol] = []

        return symbol_to_tests

    def get_changed_or_new_tests_with_info(self):
        tests = self.get_changed_tests()
        return tests, Result(
            name="changed_or_new_tests",
            status=(
                Result.StatusExtended.SKIPPED if not tests else Result.StatusExtended.OK
            ),
            info=f"Found {len(tests)} changed or new tests",
            results=[Result(name=t, status="FOUND") for t in tests],
        )

    def get_previously_failed_tests_with_info(self):
        tests = self.get_previously_failed_tests()
        # TODO: add job name to the result.info
        return tests, Result(
            name="previously_failed_tests",
            status=(
                Result.StatusExtended.SKIPPED if not tests else Result.StatusExtended.OK
            ),
            info=f"Found {len(tests)} previously failed tests",
            results=[Result(name=t, status="FOUND") for t in tests],
        )

    def get_covering_tests_with_info(self, symbols):
        """
        Generates a prioritized list of relevant tests based on changed symbols in the codebase.

        This method retrieves tests that cover the provided symbols and applies intelligent
        filtering to produce a focused test selection that maximizes coverage while staying
        within reasonable runtime constraints.

        Selection Algorithm:
        1. **Prioritize specific symbols**: Process symbols with fewer tests first, as they
           likely represent more targeted, high-value test coverage (better signal-to-noise).

        2. **Limit broad symbols**: If a symbol is covered by more than MAX_TESTS_PER_SYMBOL
           tests (indicating a very generic symbol like a common utility function), randomly
           select only RANDOM_TESTS_FOR_BROAD_SYMBOL tests to avoid test explosion.

        3. **Enforce global limit**: Stop adding tests once MAX_TESTS is reached to keep
           test suite execution time reasonable.

        4. **Final trimming**: If the accumulated tests exceed MAX_TESTS due to partial
           symbol processing, randomly sample down to exactly MAX_TESTS.

        Args:
            symbols (list): List of code symbols (function/method names) that changed

        Returns:
            set: Unique set of relevant test names to execute
        """
        import random

        MAX_TESTS = 300  # Maximum total tests to return
        MAX_TESTS_PER_SYMBOL = 100  # Threshold to identify overly broad symbols
        RANDOM_TESTS_FOR_BROAD_SYMBOL = 5  # Sample size for broad symbols

        # Deduplicate input symbols
        symbols = list(set(symbols))

        # Fetch mapping of symbols to tests from coverage database
        symbol_to_tests = self.get_tests_by_changed_symbols(symbols)
        relevant_tests = set()

        # Sort symbols by test count (ascending) - prioritize specific symbols over generic ones
        symbol_to_tests_list = sorted(symbol_to_tests.items(), key=lambda x: len(x[1]))

        not_covered_symbols = []
        results_with_info = []

        for symbol_ in symbols:
            tests_ = symbol_to_tests.get(symbol_, [])
            # Skip symbols with no test coverage
            if not tests_:
                results_with_info.append(Result(name=symbol_, status="NOT_FOUND"))
                continue

            info = f"Found {len(tests_)} tests:\n   {' '.join(tests_[0:5])} { '...' if len(tests_) > 5 else ''}"
            results_with_info.append(Result(name=symbol_, status="FOUND", info=info))

            # Handle overly broad symbols: limit to small random sample
            if len(tests_) > MAX_TESTS_PER_SYMBOL:
                tests_to_add = random.sample(
                    tests_, min(RANDOM_TESTS_FOR_BROAD_SYMBOL, len(tests_))
                )
            else:
                # For specific symbols, include all covering tests
                tests_to_add = tests_

            # Accumulate tests until we reach the limit
            for test in tests_to_add:
                if len(relevant_tests) >= MAX_TESTS:
                    break
                relevant_tests.add(test)

            # Early exit if we've reached the maximum test count
            if len(relevant_tests) >= MAX_TESTS:
                break

        # Safety check: trim to exact limit if we slightly exceeded it
        if len(relevant_tests) > MAX_TESTS:
            relevant_tests = set(random.sample(list(relevant_tests), MAX_TESTS))

        results_with_info.sort(key=lambda x: x.status, reverse=True)

        return relevant_tests, Result(
            name="found_by_coverage",
            status=(
                Result.StatusExtended.OK
                if relevant_tests
                else Result.StatusExtended.SKIPPED
            ),
            info=f"Found {len(relevant_tests)} relevant tests covering changed symbols",
            results=results_with_info,
        )

    def get_all_relevant_tests_with_info(self, ch_path):
        tests = set()
        results = []
        changed_tests, result = self.get_changed_or_new_tests_with_info()
        tests.update(changed_tests)
        results.append(result)
        previously_failed_tests, result = self.get_previously_failed_tests_with_info()
        tests.update(previously_failed_tests)
        results.append(result)
        covering_tests, result = self.get_covering_tests_with_info(
            self.get_changed_symbols(ch_path)
        )
        tests.update(covering_tests)
        results.append(result)
        return tests, Result(
            name="Fetch relevant tests",
            status=Result.Status.SUCCESS,
            info=f"Found {len(tests)} relevant tests",
            results=results,
        )


if __name__ == "__main__":
    # test:
    info = Info()
    targeting = Targeting(info)
    # symbols = targeting.get_changed_symbols(info.path)
    symbols = [
        "DB::SLRUCachePolicy<wide::integer<128ul, unsigned int>, DB::MMappedFile, UInt128TrivialHash, DB::EqualWeightFunction<DB::MMappedFile>>::clearImpl()",
        "DB::SLRUCachePolicy<wide::integer<128ul, unsigned int>, DB::MMappedFile, UInt128TrivialHash, DB::EqualWeightFunction<DB::MMappedFile>>::clearImpl()",
    ]
    symbol_to_tests = targeting.get_tests_by_changed_symbols(symbols)
    print(symbol_to_tests)
