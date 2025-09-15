from typing import Literal


ROOT_URL = lambda domain: f"https://www.vinted.{domain}"


Domain = Literal[
    "pl",
    "fr",
    "at",
    "be",
    "cz",
    "de",
    "dk",
    "es",
    "fi",
    "gr",
    "hr",
    "hu",
    "it",
    "lt",
    "lu",
    "nl",
    "pt",
    "ro",
    "se",
    "sk",
    "co.uk",
    "com",
]

SortOption = Literal[
    "relevance", "price_high_to_low", "price_low_to_high", "newest_first"
]

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
