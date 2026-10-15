import argparse

from .runtime import run_worker

parser = argparse.ArgumentParser()
parser.add_argument("--settings", required=True)
parser.add_argument("--accession", required=True)
args = parser.parse_args()
raise SystemExit(run_worker(args.settings, args.accession))
