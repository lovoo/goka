""" Reads a file line by line repeatedly and searches the first occurrence of one or
more strings. When all strings have been found, the script terminates with return code 0,
if TIMEOUT_AFTER_SECONDS seconds have passed it will terminate with return code 1.

Only used in GitHub action 'run-application'.
"""

import sys
import json
import time
import argparse

TIMEOUT_AFTER_SECONDS = 10


def wait_until_output(file_to_scan, strings_to_match):
    """Reads a file line by line repeatedly and searches the first occurrence of one or
    more strings, returns True if all have been found in less than TIMEOUT_AFTER_SECONDS,
    else False.
    """
    strings_to_match = set(strings_to_match)

    for _ in range(TIMEOUT_AFTER_SECONDS):
        with open(file_to_scan) as f:
            for line in f:
                for curr_str in list(strings_to_match):
                    if curr_str in line:
                        strings_to_match.remove(curr_str)
                if not strings_to_match:
                    return True
        time.sleep(1)

    # not all strings found, return error
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='path to file to read')
    parser.add_argument('searchstrings', help='a JSON string or array of strings')
    args = parser.parse_args()

    file_to_scan = args.file
    strings_to_match = json.loads(args.searchstrings)
    if not isinstance(strings_to_match, list):
        strings_to_match = [strings_to_match]

    ret = wait_until_output(file_to_scan, strings_to_match)

    if not ret:
        sys.exit(1)


if __name__ == '__main__':
    main()
