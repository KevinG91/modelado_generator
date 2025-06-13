import re
from typing import List


def normalizer(text: str) -> str:
    """Normalize text by converting to lowercase and replacing special characters and spaces."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)  # remove brackets, periods, etc.
    return re.sub(r"\s+", "_", text.strip())


def sorter(origin: List[str], normalized: List[str], alias: List[str]) -> List[str]:
    """Match original field names with appropriate aliases."""
    alias_map = {}
    used_aliases = set()

    for orig_raw, norm in zip(origin, normalized):
        if norm in alias and norm not in used_aliases:
            alias_map[orig_raw] = norm
            used_aliases.add(norm)
        else:
            fallback = next(
                (a for a in alias if a.startswith(norm) and a not in used_aliases), None
            )
            alias_map[orig_raw] = fallback or f"{norm}"

    aligned_aliases = [alias_map[orig] for orig in origin]
    return aligned_aliases
