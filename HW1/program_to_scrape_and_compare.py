from bs4 import BeautifulSoup
import time
from time import sleep
import requests
from random import randint
from html.parser import HTMLParser
import json
import csv

USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100Safari/537.36'}
# {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
# {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
# #{'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"}
# {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML%2C like Gecko) Chrome/97.0.4692.99 Safari/537.36 OPR/83.0.4254.62'}
class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:
            time.sleep(randint(5, 10))
        temp_url = '+'.join(query.split())
        url = 'https://www.bing.com/search?q=' + temp_url + "&count=30"
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("li", attrs={"class": "b_algo"})
        results_set = set()
        for result in raw_results:
            if result not in results_set and len(list(results_set)) < 10:
                try:
                    link = result.a['href']
                except KeyError:
                    return []
                if len(list(results_set)) < 10:
                    results_set.add(link)
        return list(results_set)


if __name__ == "__main__":
    file1 = open('./file_1.txt', 'r')    # file containing queries
    filename = './mydata1.json'          # file to write scraping results
    lines = file1.readlines()
    result = []
    for line in lines:
        results = []
        query = line.strip()
        try:
            results = SearchEngine.search(query)
            with open(filename) as fp:
                dictObj = json.load(fp)
            dictObj.update({query: results})
            with open(filename, 'w') as json_file:
                json.dump(dictObj, json_file, indent=4, separators=(',', ': '))
            result.append(results)
        except KeyError:
            print(query)
        if results:
            results = SearchEngine.search(query)
            result.append(results)

    filename1 = './mydata1.json'  # file on which scraping results were written
    filename2 = './google_results.json'    # file containg google results
    with open(filename1) as fp1:
        dictObj1 = json.load(fp1)
    with open(filename2) as fp2:
        dictObj2 = json.load(fp2)

    fields = ['Queries', 'Number of Overlapping Results', 'Percent Overlap', 'Spearman Coefficient']
    filename_csv = "./file_csv1.csv"          # file to write comparison results in csv
    with open(filename_csv, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
    csvfile.close()

    sum_number_of_overlapping_results = 0
    sum_percent_overlap = 0
    sum_spearman_coefficient = 0

    for key, value in dictObj1.items():
        value1 = value
        value2 = dictObj2[key]

        value_1 = []
        for link in value1:
            if link[-1] == '/':
                link = link[:len(link) - 1]
            if "https" in link:
                link = link[:4] + link[5:]
            if "www" in link:
                for i in range(len(link)):
                    if link[i] == "w":
                        index = i
                        break
                link = link[:index] + link[index + 4:]
            value_1 += [link]

        value_2 = []
        for link in value2:
            if link[-1] == '/':
                link = link[:len(link) - 1]
            if "https" in link:
                link = link[:4] + link[5:]
            if "www" in link:
                for i in range(len(link)):
                    if link[i] == "w":
                        index = i
                        break
                link = link[:index] + link[index + 4:]
            value_2 += [link]

        n = len(list(set(value_1) & set(value_2)))
        rho = 0
        if n > 1:
            n1 = 0
            di_square = 0
            for i in range(len(value_1)):
                if value_1[i] in value_2:
                    index1 = i + 1
                    index2 = value_2.index(value_1[i]) + 1
                    di = index2 - index1
                    di_square += (di * di)
                    n1 += 1
            rho = 1 - ((di_square * 6) / (n1 * (n1 * n1 - 1)))
        elif n == 1:
            for i in range(len(value_1)):
                if value_1[i] in value_2:
                    index1 = i + 1
                    index2 = value_2.index(value_1[i]) + 1
                    if index1 == index2:
                        rho = 1
                    else:
                        rho = 0
        else:
            rho = 0

        rows = [str(key), str(n), str(n * 10), str(rho)]
        sum_number_of_overlapping_results += n
        sum_percent_overlap += (n * 10)
        sum_spearman_coefficient += rho
        with open(filename_csv, 'a') as csvfile:
            csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
            csvwriter.writerow({'Queries': key, 'Number of Overlapping Results': str(n), 'Percent Overlap': str(n * 10),
                                'Spearman Coefficient': str(rho)})

    with open(filename_csv, 'a') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
        csvwriter.writerow(
            {'Queries': 'Averages', 'Number of Overlapping Results': str(sum_number_of_overlapping_results / 100),
             'Percent Overlap': str(sum_percent_overlap / 100),
             'Spearman Coefficient': str(sum_spearman_coefficient / 100)})

