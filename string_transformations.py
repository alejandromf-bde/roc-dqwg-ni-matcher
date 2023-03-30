# coding=utf-8

import unicodedata


def _replace_diacritics(s):
    return unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")




purge_rules = [
    lambda x: x.replace("'", ""),
    lambda x: x.replace("-", ""),
    lambda x: x.replace("_", ""),
    lambda x: x.replace("(", ""),
    lambda x: x.replace(")", ""),
    lambda x: x.replace("&", ""),
    lambda x: x.replace("+", ""),
    lambda x: x.replace(";", ""),
    lambda x: x.replace("/", ""),
    lambda x: x.replace(",", ""),
    lambda x: x.replace(".", ""),
    lambda x: x.replace('"', ""),
    lambda x: x.replace(" ", "")
]


def purge(s):
    for rule in purge_rules:
        try:
            s = rule(s)
        except AttributeError:
            pass
    return s

steps = [
    _replace_diacritics,
    purge,
]


def harmonize(s, legal_form_abb_dict):
    """
    Harmonization for company name matching inspired by
    https://github.com/Sociovestix/lenu
    Also, it uses the Legal form abbreviations from https://www.gleif.org/en/about-lei/code-lists/iso-20275-entity-legal-forms-code-list
    """
    for step in steps:
        try:
            s = step(s)
        except Exception as e:
            raise Exception("Step failed: %s - %s" % (str(step), s), e)
    for key, value in legal_form_abb_dict.items():
        s = s.replace(key, value)
    return s


