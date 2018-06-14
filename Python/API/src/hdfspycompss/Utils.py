def get_suffixes(length):
    """Create a list of suffixes to be able to merge all files after."""
    import itertools
    import math
    alpha = 'abcdefghijklmnopqrstuvwxyz'
    r = int(math.ceil(float(length)/len(alpha)))
    product = list(itertools.product(alpha, repeat=r))[:length]
    return sorted(["".join(x) for x in product])
