import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
import csv
import json
import sqlite3
import random
import logging
from tkinter import Tk, Label, Button, Entry, StringVar, filedialog, messagebox
from aiohttp import ClientSession

# 初始化日志系统
logging.basicConfig(filename='crawler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 缺省URLs
default_urls = {
    'public_opinion': 'https://news.google.com/',
    'books': 'https://www.goodreads.com/',
    'music': 'https://www.billboard.com/charts',
    'movies': 'https://www.imdb.com/chart/top',
    'vpn': 'https://www.vpngate.net/en/'
}

# 代理池（示例）
proxies = [
    {'http': 'http://proxy1:port', 'https': 'https://proxy1:port'},
    {'http': 'http://proxy2:port', 'https': 'https://proxy2:port'}
]

# 用户代理池（示例）
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

# 随机选择代理
def get_random_proxy():
    return random.choice(proxies)

# 随机选择用户代理
def get_random_user_agent():
    return random.choice(user_agents)

# 异步获取页面内容
async def fetch_page_content(session, url, retries=3):
    headers = {'User-Agent': get_random_user_agent()}
    for _ in range(retries):
        try:
            async with session.get(url, headers=headers, proxy=get_random_proxy(), timeout=10) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching page {url}: {e}")
            await asyncio.sleep(2)  # 等待后重试
    return None

# 异步爬取任务
async def perform_crawl(task_type, url=None, depth=1):
    if url is None:
        url = default_urls.get(task_type, '')
    
    if not url:
        print(f"No URL found for task type {task_type}.")
        return
    
    logging.info(f"Starting crawl task: {task_type} for URL: {url}")
    
    async with ClientSession() as session:
        html_content = await fetch_page_content(session, url)
        
        if html_content:
            data = parse_content(html_content, task_type)
            data = remove_duplicates(data)
            if data:
                filtered_data = filter_data(data, criteria=None)  # 根据需要设置筛选条件
                export_to_csv(filtered_data, task_type)
                export_to_json(filtered_data, task_type)
                save_to_db(filtered_data, task_type)
            else:
                logging.info(f"No data found for task type {task_type}.")
                print(f"No data found for task type {task_type}.")
        else:
            logging.error(f"Failed to fetch or parse data for URL: {url}")

def parse_content(html, task_type):
    soup = BeautifulSoup(html, 'html.parser')
    data = []
    
    if task_type == 'public_opinion':
        articles = soup.select('article')  # 以新闻页面为例
        for article in articles:
            title = article.find('h3')
            link = article.find('a')['href'] if article.find('a') else None
            data.append({'title': title.text if title else 'No Title', 'link': link})
    elif task_type == 'books':
        books = soup.select('.bookTitle')
        for book in books:
            title = book.text.strip()
            data.append({'title': title})
    elif task_type == 'music':
        songs = soup.select('.chart-element__information__song')
        for song in songs:
            data.append({'song': song.text.strip()})
    elif task_type == 'movies':
        movies = soup.select('.titleColumn a')
        for movie in movies:
            title = movie.text.strip()
            data.append({'movie': title})
    elif task_type == 'vpn':
        vpn_entries = soup.select('table#vg_hosts_table tbody tr')
        for vpn in vpn_entries:
            country = vpn.find_all('td')[1].text.strip()
            ip = vpn.find_all('td')[0].text.strip()
            data.append({'country': country, 'ip': ip})
    
    return data

def remove_duplicates(data):
    seen = set()
    unique_data = []
    for item in data:
        item_tuple = tuple(item.items())  # 转换为可哈希的元组
        if item_tuple not in seen:
            unique_data.append(item)
            seen.add(item_tuple)
    return unique_data

def filter_data(data, criteria):
    filtered_data = [item for item in data if meets_criteria(item, criteria)]
    return filtered_data

def meets_criteria(item, criteria):
    # 根据条件检查数据项是否符合
    # 示例条件，实际应用中需根据需求实现
    return True  # 示例条件

def export_to_csv(data, task_type):
    filename = f'{task_type}_data_{time.strftime("%Y%m%d_%H%M%S")}.csv'
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Data exported to {filename}")
    print(f"Data successfully exported to {filename}")

def export_to_json(data, task_type):
    filename = f'{task_type}_data_{time.strftime("%Y%m%d_%H%M%S")}.json'
    with open(filename, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=4)
    logging.info(f"Data exported to {filename}")
    print(f"Data successfully exported to {filename}")

def save_to_db(data, task_type):
    conn = sqlite3.connect('crawler_data.db')
    c = conn.cursor()
    
    # 创建表格（如果不存在）
    if task_type == 'public_opinion':
        c.execute('''CREATE TABLE IF NOT EXISTS public_opinion
                     (title TEXT, link TEXT)''')
    elif task_type == 'books':
        c.execute('''CREATE TABLE IF NOT EXISTS books
                     (title TEXT)''')
    elif task_type == 'music':
        c.execute('''CREATE TABLE IF NOT EXISTS music
                     (song TEXT)''')
    elif task_type == 'movies':
        c.execute('''CREATE TABLE IF NOT EXISTS movies
                     (movie TEXT)''')
    elif task_type == 'vpn':
        c.execute('''CREATE TABLE IF NOT EXISTS vpn
                     (country TEXT, ip TEXT)''')
    
    # 插入数据
    for item in data:
        if task_type == 'public_opinion':
            c.execute('INSERT INTO public_opinion (title, link) VALUES (?, ?)', (item['title'], item['link']))
        elif task_type == 'books':
            c.execute('INSERT INTO books (title) VALUES (?)', (item['title'],))
        elif task_type == 'music':
            c.execute('INSERT INTO music (song) VALUES (?)', (item['song'],))
        elif task_type == 'movies':
            c.execute('INSERT INTO movies (movie) VALUES (?)', (item['movie'],))
        elif task_type == 'vpn':
            c.execute('INSERT INTO vpn (country, ip) VALUES (?, ?)', (item['country'], item['ip']))
    
    conn.commit()
    conn.close()
    logging.info("Data saved to database")
    print("Data successfully saved to database")

# GUI 部分
def start_crawl():
    task_type = task_type_var.get()
    url = url_entry.get() or None
    depth = int(depth_var.get() or 1)
    if not task_type:
        messagebox.showerror("Input Error", "Please select a task type.")
        return
    
    asyncio.run(perform_crawl(task_type, url, depth))

def create_gui():
    root = Tk()
    root.title("Web Crawler")

    Label(root, text="Task Type:").grid(row=0, column=0, padx=10, pady=5)
    Label(root, text="URL:").grid(row=1, column=0, padx=10, pady=5)
    Label(root, text="Depth:").grid(row=2, column=0, padx=10, pady=5)

    global task_type_var, url_entry, depth_var
    task_type_var = StringVar()
    url_entry = Entry(root, width=50)
    depth_var = StringVar()

    Entry(root, textvariable=task_type_var).grid(row=0, column=1, padx=10, pady=5)
    url_entry.grid(row=1, column=1, padx=10, pady=5)
    Entry(root, textvariable=depth_var).grid(row=2, column=1, padx=10, pady=5)

    Button(root, text="Start Crawl", command=start_crawl).grid(row=3, column=0, columnspan=2, pady=10)

    root.mainloop()

# 启动 GUI
if __name__ == "__main__":
    create_gui()