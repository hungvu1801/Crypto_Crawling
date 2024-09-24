COMPANY = "OKX"

"GET https://www.okx.com/priapi/v5/ecotrade/public/preference-coin?latestNum=0&uniqueName=74DF4659E1627676" # get coins

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en,en-CA;q=0.9,vi;q=0.8,en-US;q=0.7,en-GB;q=0.6",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Google Chrome\";v=\"127\", \"Chromium\";v=\"127\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "user-agent": ""
}

url_api_get_pnl = "https://www.okx.com/priapi/v5/ecotrade/public/yield-pnl?latestNum=7&uniqueName="

headers_position = {
    'Host': 'www.okx.com',
    'User-Agent': '',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': '',
    'App-Type': 'web',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0',
    'TE': 'trailers'
}

url_api_get_history = "https://www.okx.com//priapi/v5/ecotrade/public/position-history?"