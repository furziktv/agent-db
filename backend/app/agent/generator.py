import random
import string

ALPHABET = string.ascii_letters + string.digits + ' '

def payload(w, rng: random.Random) -> str:
    lo, hi = w.min_payload, w.max_payload
    mode = w.payload_distribution
    if mode == 'SMALL_HEAVY':
        size = int(lo+(hi-lo)*rng.betavariate(1,5))
    elif mode == 'LARGE_HEAVY':
        size = int(lo+(hi-lo)*rng.betavariate(5,1))
    elif mode == 'RANDOM':
        size = rng.choice((lo, hi, rng.randint(lo, hi)))
    else:
        size = rng.randint(lo, hi)
    return ''.join(rng.choices(ALPHABET, k=size))
