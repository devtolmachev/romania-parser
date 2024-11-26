import asyncio
import json
import pickle

from numpy import random
import pandas as pd
import bs4
import aiofiles
from aiohttp import ClientSession
import ua_generator

class RomanianPassportAPI:
    
    @property
    def _hdrs(self) -> dict:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,my;q=0.6',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'referer': 'https://romanian-passport.com/dom22d/left.asp',
            'sec-fetch-dest': 'frame',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
        }
        for k, v in ua_generator.generate().headers.get().items():
            headers[k] = v
        return headers
    
    async def get_data(self, page: int = 0, proxy: str | None = None, timeout: int = 10):
        url = 'https://romanian-passport.com/dom22d/newsletters.asp'
        cookies = {
            'Referral%5FType': '5',
            'Return%5FID': '1329402',
            'ASPSESSIONIDAWQBSQTC': 'OAIPAFLBKHNHMFAPNPKEJLGD',
            'ASPSESSIONIDSWRAQQQC': 'HIKHPNFCIEDPCKIBCKCKJNHJ',
        }
        headers = self._hdrs
        
        async with ClientSession() as session:
            if not page or page == 1:
                async with session.get(url, timeout=timeout, headers=headers, cookies=cookies, proxy=proxy, ssl=False) as resp:
                    response = await resp.text()
            else:
                data = {
                    'Action': '',
                    'OpenByID': '',
                    'Newsletter_Status': '0',
                    'Newsletter_SubStatus': '0',
                    'Newsletter_Type': '0',
                    'Newsletter_Archived': '1',
                    'Newsletter_AddedByID': '0',
                    'Newsletter_ActionList': '',
                    'SearchBy': '3',
                    'SearchMatch': '3',
                    'Keywords': '',
                    'SearchByDate': '1',
                    'FromDate': '',
                    'ToDate': '',
                    'SortBy': '',
                    'SortDir': '0',
                    'PageMode': 'm',
                    'Print': '',
                    'IDs': '',
                    'PageNo': str(page),
                }
                async with session.post(url, timeout=timeout, data=data, headers=headers, cookies=cookies, proxy=proxy) as resp:
                    response = await resp.text()
        
        # with open('resp.pkl', 'rb') as f:
        #     response = pickle.load(f)
        
        df = await asyncio.to_thread(self._parse_table, response)
        return df
    
    def _parse_table(self, html: str):
        soup = bs4.BeautifulSoup(html, "lxml")
        data = []
        table = soup.find('table', attrs={'class':'list'})
        table_body = table.find('tbody')
        table_head = table.find('thead')
        
        heads = []
        for el in table_head.find_all("th")[1:]:
            heads.append(el.text.strip())
        heads.append('Описание')

        rows = table_body.find_all('tr')
        for i, row in enumerate(rows):
            cols_tags = row.find_all('td')
            cols = []
            for ele in cols_tags[1:]:
                div = ele.find('div')
                if not div or not div.get('title'):
                    ele: bs4.Tag
                    text = ele.get_text(strip=True)
                else:
                    text = div.get('title').strip()
                
                text = text.replace('\xa0', ' ').replace('\n', ' ').strip()
                cols.append(text)
            
            if len(cols) == 1:
                data[len(data)-1].extend(cols)
            else:
                data.append(cols)

        df = pd.DataFrame(data, columns=heads)
        return df


async def write(df: pd.DataFrame):
    statuses = df['Статус'].unique()
    
    try:
        with pd.ExcelFile('users_status.xlsx') as xls:
            existing_sheets = xls.sheet_names
    except FileNotFoundError:
        existing_sheets = []
    
    with pd.ExcelWriter('users_status.xlsx', engine='openpyxl', mode='a' if existing_sheets else 'w', if_sheet_exists="overlay") as writer:
        for status in statuses:
            # Фильтруем данные по текущему статусу
            filtered_df = df[df['Статус'] == status]
            
            # Записываем отфильтрованный DataFrame на отдельный лист
            # Убедитесь, что index=False и header=True (по умолчанию)
            if status in existing_sheets:
                existing_df = pd.read_excel(writer, sheet_name=status)
                combined_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                combined_df.to_excel(writer, sheet_name=status, index=False)
            else:
                # Записываем отфильтрованный DataFrame на новый лист
                filtered_df.to_excel(writer, sheet_name=status, index=False)


async def work():
    with open('proxies.json') as f:
        proxies = json.load(f)
    
    dfs = []
    cached_pages = []
    with open('cached_pages.pkl', 'rb') as f:
        cached_pages = pickle.load(f)
    
    async def gather(page, proxy):
        nonlocal dfs, cached_pages
        api = RomanianPassportAPI()
        
        df = await api.get_data(page=page, proxy=proxy, timeout=10)
        dfs.append(df)
        cached_pages.append(page)

    tasks = []
    empty = False
    for i in range(1, 6559):
        if i in cached_pages:
            continue
        
        if not empty:
            empty = True
            
        if len(tasks) == 30:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            with open('cached_pages.pkl', 'wb') as f:
                pickle.dump(cached_pages, f)
                
            df = pd.concat(dfs)
            await write(df)
            tasks.clear()
            await asyncio.sleep(5)
            
        proxy = random.choice(proxies)
        proxy += str(random.randint(10000, 10999))
        
        task = gather(i, proxy)
        tasks.append(task)
    
    if not empty:
        raise ValueError('empty')
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    df = pd.concat(dfs)
    await write(df)
    ...


async def main():
    proxy = 'http://UK5Wgiu76ziq:RNW78Fm5@pool.proxy.market:10000'
    
    while True:
        await work()
    ...

        
if __name__ == "__main__":
    asyncio.run(main())