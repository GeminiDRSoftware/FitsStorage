#!/bin/python

import requests

import concurrent.futures
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print('%d is prime: %s' % (number, prime))


urls = [
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-CAL-171-20",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-CAL-171-19",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-CAL-171-21",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-CAL-171-17",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-Q-123-6",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2019B-FT-107-7",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2019B-Q-235-11",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2019B-Q-235-12",
    "/jsonqastate/Gemini-North/present/RAW/N20200323",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2019B-Q-235-13",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2019B-Q-235-14",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-5",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-6",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-7",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-13",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-9",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-37",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-39",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-40",
    "/jsonqastate/Gemini-North/present/RAW/obsid=GN-2018B-LP-102-41"
]


if __name__ == "__main__":
    print("thrash test of archive")
    url = "http://mkofits-lv3.hi.gemini.edu%s" % urls[0]
    r = requests.get(url)
    if r.status == 200:
        print("ok")
    else:
        print("err")
