import argparse
import asyncio
import logging

from ..db import pool
from ..migrate import run_migrations
from .evaluate import evaluate
from .labeling import export_labels, import_labels, run_label
from .report import summary, top
from .scan import run_loop, run_scan


async def dispatch(args):
    await pool.open()
    try:
        await run_migrations()
        if args.command == "scan":
            await run_scan(args.company_id)
        elif args.command == "loop":
            await run_loop(args.every)
        elif args.command == "label":
            await run_label(args.count)
        elif args.command == "eval":
            await evaluate(args.errors, args.json)
        elif args.command == "export":
            await export_labels(args.path)
        elif args.command == "import":
            await import_labels(args.path)
        elif args.command == "top":
            await top(args.limit, args.min)
        elif args.command == "summary":
            await summary()
    finally:
        await pool.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="lively.ghost")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scan = sub.add_parser("scan")
    p_scan.add_argument("--company-id", type=int)

    p_loop = sub.add_parser("loop")
    p_loop.add_argument("--every", type=int, default=600)

    p_label = sub.add_parser("label")
    p_label.add_argument("--count", type=int, default=200)

    p_eval = sub.add_parser("eval")
    p_eval.add_argument("--errors", type=int, default=0)
    p_eval.add_argument("--json", action="store_true")

    p_export = sub.add_parser("export")
    p_export.add_argument("path")

    p_import = sub.add_parser("import")
    p_import.add_argument("path")

    p_top = sub.add_parser("top")
    p_top.add_argument("--limit", type=int, default=15)
    p_top.add_argument("--min", type=float, default=0.6)

    sub.add_parser("summary")

    asyncio.run(dispatch(parser.parse_args()))


if __name__ == "__main__":
    main()