import hashlib

from ..normalize import norm

BITS = 64
MAX_WORDS = 800
SHINGLE = 3
KEEP_MASK = 3


def _hash(token):
    return int.from_bytes(hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest(), "big")


def simhash(text):
    words = norm((text or "")[:8000]).split()[:MAX_WORDS]
    if not words:
        return 0
    if len(words) <= SHINGLE:
        grams = [" ".join(words)]
    else:
        grams = [" ".join(words[i : i + SHINGLE]) for i in range(len(words) - SHINGLE + 1)]
    hashes = {_hash(g) for g in grams}
    kept = [h for h in hashes if (h & KEEP_MASK) == 0] or list(hashes)
    counts = [0] * BITS
    for h in kept:
        for bit in range(BITS):
            counts[bit] += 1 if (h >> bit) & 1 else -1
    out = 0
    for bit in range(BITS):
        if counts[bit] > 0:
            out |= 1 << bit
    return out


def to_signed(value):
    return value - (1 << BITS) if value >= (1 << (BITS - 1)) else value


def to_unsigned(value):
    return value + (1 << BITS) if value < 0 else value


def distance(a, b):
    return (a ^ b).bit_count()