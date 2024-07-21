import random

DIR = "workloads"
SALT = 433  # must be prime
TABLE = "t"

def write(lines, fn):
    with open(fn, "w") as f:
        f.write("\n".join(lines))

def getKey(mode, n):
    if mode == "increasing":
        return lambda x: x
    elif mode == "decreasing":
        return lambda x: n - x
    elif mode == "unordered" or mode == "chaotic":
        return lambda x: x * SALT % n
    else:
        raise "received bad mode"

def getValue():
    return 0

def getEntries(op, k, v):
    ret = []
    if op == "insert" or op == "all":
        ret.append(f"insert {k} {v} into {TABLE}")
    if op == "all":
        ret.append(f"delete {k} from {TABLE}")
    return ret

def gen(m, op, n):
    lines = []
    for i in range(n):
        k, v = getKey(m, n)(i), getValue()
        lines += getEntries(op, k, v)
    if m == "chaotic":
        random.shuffle(lines)
    return lines

if __name__ == "__main__":
    MODES = ["increasing", "decreasing", "unordered", "chaotic"]
    OPS = ["insert", "all"]
    NS = [("sm", 100), ("md", 1000), ("lg", 10000)]
    for m in MODES:
        for op in OPS:
            for n in NS:
                fn = f"./{DIR}/{m[0]}-{op[0]}-{n[0]}.txt"
                lines = gen(m, op, n[1])
                write(lines, fn)
