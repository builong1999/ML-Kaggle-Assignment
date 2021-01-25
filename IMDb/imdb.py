#Web Access and parsing
import urllib.parse
from bs4 import BeautifulSoup
import requests
import numpy as np 
import pandas as pd 
from threading import Thread
import threading
import math
import time
from IPython.display import display

dataset_path = './netflix_titles_imdb.csv'

data = pd.read_csv(dataset_path)

data = data[data["IMDb"]>10]
display(data)



headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
thread_num = 8
suggest_imdb = 0

def main():
    print("Starting crawl data")
    num_shows = int(len(data.index))
    print(num_shows)
    imdb_list = list(range(0,num_shows))
    steps = []
    start = 0
    end  = 0
    start_time = time.time()

    condition = num_shows%thread_num == 0
    loop = thread_num if condition else thread_num -1
    
    each_row = int(num_shows/thread_num if condition else math.ceil(num_shows/thread_num))
    for index in range(0,loop):
        end = start+each_row-1
        steps.append((start,end))
        start = end+1
        
    if not condition:
        steps.append((start,num_shows))
    print(steps)

    lst_thread = []
    for thread in steps:
        print('Start Thread')
        th = threading.Thread(target=execute_analyze_data,args=(thread[0],thread[1],imdb_list,))
        th.start()
        lst_thread.append(th)
    
    for th in lst_thread:
        th.join()

    data.insert(2, "IMDb", imdb_list, True) 
    data.to_csv('data_with_imdb_rear.csv', sep=',', encoding='utf-8', index=False)
    print("Done crawl data: ", time.time() - start_time, "s")
    select_top_imdb(data, suggest_imdb)

def select_top_imdb(data, suggest_imdb):
    print("Starting select suggestion show")
    select = data.loc[data["IMDb"] >= suggest_imdb]
    select.to_csv('suggest_movie_rear.csv', sep=',', encoding='utf-8', index=False)
    # print("Done: ", time.time() - now, "s")

def execute_analyze_data(start,end, imdb_list):
    href_link = get_list_id(data,start,end)
    print('----------------', len(href_link),'-----------------------')
    get_imdb_rate(imdb_list, data,href_link,start,end)

def get_list_id(data, start, end):
    href_link = []
    for index in range(start,end):
        print(index,"-Process get title id: ", data["title"][index], end="\n")

        values = {'q':data["title"][index]} 
        query = urllib.parse.urlencode(values) 
        query_find = 'https://imdb.com/find?{}'.format(query)
        response = requests.get(query_find, headers=headers)
        resp = response.content
        html = BeautifulSoup(resp,'html.parser')
        result = html.findAll("td", {"class": "result_text"})
        if len(result) > 0:
            isNew = False
            for item in result:
                if item.text.find("({})".format(data["release_year"])):
                    href_link.append(item.a["href"])
                    isNew = True
                    break;
            if not isNew:
                href_link.append(None)
                continue
        else:
            href_link.append(None)
    return href_link

def get_imdb_rate(imdb_list, data, href_link, start, end):
    i = 0
    for index in range(start,end):
        print(index,"Process get imdb: ", data["title"][index],  end="\n")
        if href_link[i]:
            query_find = 'https://imdb.com{}'.format(href_link[i])
            response = requests.get(query_find, headers=headers)
            resp = response.content
            html = BeautifulSoup(resp,'html.parser')
            result = html.findAll("div", {"class": "ratingValue"})
            if len(result) > 0:
                score = float(result[0].strong.span.text)
            else:
                score = None
        else:
            score = None
        imdb_list[index] = score
        i +=1
    return imdb_list

def update_imdb_rate(imdb_list, data, href_link, start, end):
    i = 0
    for index in range(start,end):
        print(index,"Process get imdb: ", data["title"][index],  end="\n")
        if href_link[i]:
            query_find = 'https://imdb.com{}'.format(href_link[i])
            response = requests.get(query_find, headers=headers)
            resp = response.content
            html = BeautifulSoup(resp,'html.parser')
            result = html.findAll("div", {"class": "ratingValue"})
            if len(result) > 0:
                score = float(result[0].strong.span.text)
            else:
                score = None
        else:
            score = None
        imdb_list[index] = score
        i +=1
    return imdb_list

# if __name__ == "__main__":
#     main()
