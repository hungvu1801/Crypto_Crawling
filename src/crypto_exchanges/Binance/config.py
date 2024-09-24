COMPANY = "Binance"
url_api = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list"
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=UTF-8",
    "DNT": "1",
    "Host": "www.binance.com",
    "Origin": "null",
    "Priority": "\"u=0, i\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "TE": "trailers",
    "Sec-GPC": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
}

payload = {
    "pageNumber":1,
    "pageSize":18,
    "timeRange":"7D",
    "dataType":"PNL",
    "favoriteOnly":"false",
    "hideFull":"true",
    "nickname":"",
    "order":"DESC",
    "userAsset":0,
    "portfolioType":"PUBLIC"
}

portfolios = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=932006188448096257"

portfolio_coin = "https://www.binance.com/bapi/futures/v1/public/future/copy-trade/lead-portfolio/performance/coin?portfolioId=3972242089544603393&timeRange=7D"

api_performance_mdd = "https://www.binance.com/bapi/futures/v1/public/future/copy-trade/lead-portfolio/performance?portfolioId=3972242089544603393&timeRange=7D"

url_api_get_history = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history"
headers_position = {
    "Host": "www.binance.com",
    "User-Agent": "",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "",
    "lang": "en",
    "content-type": "application/json",
    "clienttype": "web",
    "devicelanguage": "en",
    "language": "en_US",
    "locale": "en_US",
    "Origin": "https://www.binance.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Alt-Used": "www.bitget.com",
    "Priority": "u=0",
    "TE": "trailers"
}