from urllib.parse import quote

def quote_url(org, to_add):
    return org + quote(to_add)