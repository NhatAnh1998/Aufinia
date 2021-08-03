from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from difflib import SequenceMatcher
from selenium.webdriver.chrome.options import Options
import requests
import pandas as pd
import time


def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en-US"})
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    return webdriver.Chrome(executable_path='C:/Users/ADMIN/Downloads/ChromeDriver/chromedriver.exe', options=chrome_options)

df_company = pd.read_excel("C:/Users/ADMIN/OneDrive/Work/Total_Project/WebSrapping/FILE_TO_CHECK/CHECK_SWISS.xlsx", converters={'LFA1_LIFNR':str,'LFA1_STCD1':str},usecols={'LFA1_LIFNR','LFA1_NAME1','COUNTRY_NAME','LFA1_STCD1','LFA1_LAND1'})

# df_company_isnull = df_company.loc[(df_company['LFA1_LAND1'] == 'FR') & (df_company['LFA1_STCD1'].isnull() == True)]
# df_company_isnull.reset_index(drop=True, inplace = True) 
# df_company_isnull.shape

# print(df_company.head())

driver = create_driver()
driver.get('https://bizdirectasia.com/search/company/0ZntKF4TF6FmMN3Q====')