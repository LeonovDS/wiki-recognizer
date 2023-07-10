import csv
import asyncio 
import aiohttp
from bs4 import BeautifulSoup

urls_queue = asyncio.Queue(maxsize=100)
file_queue = asyncio.Queue(maxsize=100)

sem = asyncio.Semaphore(36)

def load_languages(path):
    languages = []
    with open(path, 'r') as labels:
        reader = csv.reader(labels, delimiter=';')
        next(reader)
        for row in reader:
            languages.append(row[2])
    return languages

async def generate_urls(session, languages):
    while True:
        for lang in languages:
            await urls_queue.put((lang, f'http://{lang}.wikipedia.org/wiki/Special:Random'))
            print(f'Generated url {lang}')
            asyncio.create_task(load_page(session))

async def write_to_file(file):
    while True:
        try:
            item = await file_queue.get()
            print(f'Writing {item[0]} {str(item[1])[:20]}')
            file.write(item[0] + ';' + item[1] + '\n')
        except Exception:
            pass

async def load_page(session):
    async with sem:
        lang, url = await urls_queue.get()
        page = await session.get(url)
        page_text = await page.text()
        print(f'Loaded page {lang}')
        soup = BeautifulSoup(page_text, 'lxml')
        data = soup.find(id="mw-content-text")
        data = data.find_all('p')
        text = ' '.join([p.text.strip() for p in data])
        text = text.replace('\n', ' ').replace('\r', ' ').replace(';', ' ')
        await file_queue.put((lang, text))

async def main():
    languages = load_languages('data/labels.csv')
    writer = asyncio.create_task(write_to_file(open('data.txt', 'w')))
    print("Created writer")
    async with aiohttp.ClientSession() as session:
        print("Created session")
        asyncio.create_task(generate_urls(session, languages))
        print("Created generator")
        await asyncio.wait([writer])

if __name__ == '__main__':
    asyncio.run(main())