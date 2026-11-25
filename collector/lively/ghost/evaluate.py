import json

from ..db import pool

CLASSES = ["live", "closed", "ghost", "repost"]
BAD = {"closed", "ghost"}

QUERY = """
SELECT l.job_id, l.label::text, j.predicted_label::text, j.ghost_score,
       j.ghost_reasons, j.url, j.title
FROM labels l JOIN jobs j ON j.id = l.job_id
WHERE j.predicted_label IS NOT NULL
ORDER BY l.job_id
"""


def _ratio(a, b):
    return a / b if b else None


def _f1(precision, recall):
    if precision is None and recall is None:
        return None
    p = precision or 0.0
    r = recall or 0.0
    return 0.0 if p + r == 0 else 2 * p * r / (p + r)


def metrics(pairs):
    matrix = {a: {p: 0 for p in CLASSES} for a in CLASSES}
    for actual, predicted in pairs:
        matrix[actual][predicted] += 1
    per_class = {}
    for c in CLASSES:
        tp = matrix[c][c]
        fp = sum(matrix[a][c] for a in CLASSES if a != c)
        fn = sum(matrix[c][p] for p in CLASSES if p != c)
        precision = _ratio(tp, tp + fp)
        recall = _ratio(tp, tp + fn)
        per_class[c] = {
            "precision": precision,
            "recall": recall,
            "f1": _f1(precision, recall),
            "support": tp + fn,
        }
    total = len(pairs)
    correct = sum(matrix[c][c] for c in CLASSES)
    tp = sum(1 for a, p in pairs if a in BAD and p in BAD)
    fp = sum(1 for a, p in pairs if a not in BAD and p in BAD)
    fn = sum(1 for a, p in pairs if a in BAD and p not in BAD)
    f1s = [v["f1"] for v in per_class.values() if v["support"] and v["f1"] is not None]
    return {
        "total": total,
        "accuracy": _ratio(correct, total),
        "macro_f1": sum(f1s) / len(f1s) if f1s else None,
        "matrix": matrix,
        "per_class": per_class,
        "bad_filter": {
            "precision": _ratio(tp, tp + fp),
            "recall": _ratio(tp, tp + fn),
            "support": tp + fn,
        },
    }


def _fmt(value):
    return "-" if value is None else f"{value:.2f}"


def render(result):
    lines = ["confusion matrix (rows = hand label, columns = predicted)"]
    lines.append(" " * 10 + "".join(f"{c:>9}" for c in CLASSES))
    for a in CLASSES:
        lines.append(f"{a:<10}" + "".join(f"{result['matrix'][a][p]:>9}" for p in CLASSES))
    lines.append("")
    lines.append(f"{'class':<10}{'precision':>10}{'recall':>9}{'f1':>7}{'support':>9}")
    for c in CLASSES:
        v = result["per_class"][c]
        lines.append(
            f"{c:<10}{_fmt(v['precision']):>10}{_fmt(v['recall']):>9}{_fmt(v['f1']):>7}{v['support']:>9}"
        )
    b = result["bad_filter"]
    lines.append("")
    lines.append(f"labeled jobs        {result['total']}")
    lines.append(f"accuracy            {_fmt(result['accuracy'])}")
    lines.append(f"macro f1            {_fmt(result['macro_f1'])}")
    lines.append(
        f"bad-job filter      precision {_fmt(b['precision'])} | recall {_fmt(b['recall'])} "
        f"| support {b['support']}  (closed or ghost caught before reaching a user)"
    )
    lines.append("")
    lines.append("the labeled set is stratified, so these are sample numbers, not population rates")
    return "\n".join(lines)


async def evaluate(show_errors=0, as_json=False):
    async with pool.connection() as conn:
        cur = await conn.execute(QUERY)
        rows = await cur.fetchall()
    if not rows:
        print("no labeled jobs with predictions yet: run scan, then label")
        return
    result = metrics([(r[1], r[2]) for r in rows])
    if as_json:
        print(json.dumps(result, indent=2))
    else:
        print(render(result))
    if show_errors:
        wrong = [r for r in rows if r[1] != r[2]][:show_errors]
        print(f"\nmisclassified ({len(wrong)} shown)")
        for jid, actual, predicted, score, reasons, url, title in wrong:
            print(f"\n#{jid} labeled {actual}, predicted {predicted} (score {score:.2f}) | {title}")
            print(f"   {url}")
            for reason in reasons:
                print(f"   - {reason}")