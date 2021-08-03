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
import pyperclip
import pandas as pd
import random
import time



def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en-US"})
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    return webdriver.Chrome(executable_path='C:/Users/ADMIN/Downloads/ChromeDriver/chromedriver.exe', options=chrome_options)


df_company = pd.read_excel(r"C:\Users\ADMIN\Documents\Aufinia\Mautirius_Large_Half2\LargeCountries_Half2.xlsx",sheet_name='Sheet1', converters={'LFA1_LIFNR':str,'LFA1_STCD1':str},usecols={'LFA1_LIFNR','LFA1_NAME1','COUNTRY','LFA1_STCD1','LFA1_LAND1'})

# df_company_isnull = df_company.loc[(df_company['LFA1_LAND1'] == 'FR') & (df_company['LFA1_STCD1'].isnull() == True)]
# df_company_isnull.reset_index(drop=True, inplace = True) 
# df_company_isnull.shape

# print(df_company.head())
# print(df_company.info())


driver = create_driver()
driver.get('https://www.sirene.fr/sirene/public/recherche')

for idx in range(66,len(df_company)):

    try:

        tax_number = df_company.loc[idx,'LFA1_STCD1']   

        time.sleep(3)
        tax_number_input = driver.find_element(By.ID, "sirenSiretQuery")
        tax_number_input.clear()
        tax_number_input.send_keys(str(tax_number))

        driver.implicitly_wait(10)
        search_button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.ID, "btn-search")))
        driver.execute_script("arguments[0].click();", search_button)
        print(str(idx) +": "+str(tax_number))

        WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='collapse-0']")))

        df_company.loc[idx,'Name_search'] = driver.find_element(By.XPATH, '//*[@id="collapse-0"]/div/div[2]/p[3]/font/font').text.strip()
        df_company.loc[idx,'Address_search'] = driver.find_element(By.XPATH, '//*[@id="collapse-0"]/div/div[1]/div[4]/p/font[4]/font').text.strip()
        df_company.loc[idx,'Tax_code_search'] = driver.find_element(By.XPATH, '//*[@id="collapse-0"]/div/div[2]/p[6]/font/font[2]').text.strip()
        df_company.loc[idx,'Industry'] = driver.find_element(By.XPATH,  '//*[@id="collapse-0"]/div/div[1]/div[5]/p/font/font').text.strip()
        df_company.loc[idx,'URLs'] = driver.current_url

        time.sleep(2)
        # Get back to previous page

    except NoSuchElementException as exception:
        df_company.loc[idx,'Exception'] = "No information found"
        continue

    finally:
        # df_company.to_excel(r"C:\Users\ADMIN\Documents\Aufinia\Mautirius_Large_Result.xlsx",sheet_name='Sheet1') 
        df_company.to_excel(r"C:\Users\ADMIN\Documents\Aufinia\Mautirius_Large_Half2\LargeCountries_Result2.xlsx",sheet_name='Sheet1') 

print(df_company)
driver.close()
   