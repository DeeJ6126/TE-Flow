import argparse

from .runtime import run_scheduler

parser = argparse.ArgumentParser()
parser.add_argument("--settings", required=True)
args = parser.parse_args()
raise SystemExit(run_scheduler(args.settings))
