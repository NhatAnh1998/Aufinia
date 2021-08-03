
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from difflib import SequenceMatcher
from selenium.webdriver.chrome.options import Options
import requests
import pandas as pd
import time


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en-US"})
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    return webdriver.Chrome(executable_path='C:/Users/ADMIN/Downloads/ChromeDriver/chromedriver.exe', options=chrome_options)


df_company = pd.read_excel(r"C:\Users\ADMIN\Documents\Aufinia\Algieria\Algieria_half1.xlsx", converters={'LFA1_LIFNR':str,'LFA1_STCD1':str},usecols={'LFA1_LIFNR','LFA1_NAME12','COUNTRY','LFA1_STCD1','LFA1_LAND1'})

# df_company_isnull = df_company.loc[(df_company['LFA1_STCD1'].isnull() == True)]
# df_company_isnull.reset_index(drop=True, inplace = True) 
# df_company_isnull.shape

# Create driver
driver = create_driver()


for idx in range(len(df_company)):

    try:
        # get company name
        company_name = df_company.loc[idx,'LFA1_NAME12'].replace(' ','+').replace('&','%26')
        print(str(idx) + ": " + company_name)  
        driver.get('https://www.google.com/search?q='+company_name)
        driver.implicitly_wait(10)

        # Search
        df_company.loc[idx,'First result'] = driver.find_element(By.XPATH, '(//div[@class="yuRUbf"])[1]/a/h3').text          
        df_company.loc[idx,'First Website'] = driver.find_element(By.XPATH, '(//div[@class="yuRUbf"])[1]/a').get_attribute("href")
        df_company.loc[idx,'Second result'] = driver.find_element(By.XPATH, '(//div[@class="yuRUbf"])[2]/a/h3').text            
        df_company.loc[idx,'Second Website'] = driver.find_element(By.XPATH, '(//div[@class="yuRUbf"])[2]/a').get_attribute("href")

        search_name = df_company.loc[idx,'LFA1_NAME12']
        search_result = df_company.loc[idx,'First result']
        search_result2 = df_company.loc[idx,'Second result']

        df_company.loc[idx,'Similiar_check1'] = similar(search_name,search_result)
        df_company.loc[idx,'Similiar_check2'] = similar(search_name,search_result2)

        time.sleep(2)
        # Get back to previous page
        driver.execute_script("window.history.go(-1)")


    except NoSuchElementException as exception:
        df_company.loc[idx,'Exception'] = "No information found"
        continue

    finally:
        df_company.to_excel(r"C:\Users\ADMIN\Documents\Aufinia\Algieria\Algieria_result_half1.xlsx",sheet_name='Sheet1') 


print(df_company)
driver.close()





