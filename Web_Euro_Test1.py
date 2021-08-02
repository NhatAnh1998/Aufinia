from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import pyperclip
import pandas as pd
import time



def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en-US"})
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    return webdriver.Chrome(executable_path='C:/Users/ADMIN/Downloads/ChromeDriver/chromedriver.exe', options=chrome_options)


df_company = pd.read_excel(r"C:\Users\ADMIN\Documents\Aufinia\Algieria\LFA1_T077Y_ALGIERIA.xlsx",sheet_name='Euro_ExcludeDup', converters={'LFA1_LIFNR':str,'LFA1_STCD1':str},usecols={'LFA1_LIFNR','LFA1_NAME1','COUNTRY','LFA1_STCD1','LFA1_LAND1'})

# df_company_isnull = df_company.loc[(df_company['LFA1_LAND1'] == 'FR') & (df_company['LFA1_STCD1'].isnull() == True)]
# df_company_isnull.reset_index(drop=True, inplace = True) 
# df_company_isnull.shape

# print(df_company.head())
# print(df_company.info())


driver = create_driver()
driver.get('https://www.hithorizons.com/search?Name=')

for idx in range(42,len(df_company)):
   

    try:
            # get company_name, country_name from dataframe
            company_name = df_company.loc[idx,'LFA1_NAME1']
            country_name = df_company.loc[idx,'COUNTRY']

            time.sleep(5)

            # send company_name keys
            company_name_input = driver.find_element(By.ID, "Name")
            company_name_input.clear()
            company_name_input.send_keys(company_name)


            # send country_name keys
            country_name_input = driver.find_element(By.ID, "Address")
            country_name_input.clear()
            country_name_input.send_keys(country_name)

            
            #click search button
            search_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='main-content']/div[1]/div/form/div[1]/div[3]/button")))
            driver.execute_script("arguments[0].click();", search_button)


            
            print(str(idx) +": "+str(company_name)) 

            time.sleep(5)
            #click first href link
            first_result = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='main-content']/div[2]/div[1]/div/div[1]/div[1]/div[1]/h3/a")))
            first_result.click()

            
            driver.implicitly_wait(10)
            # click copy clipboard
            clipboard_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chart-content']/div/div/div[1]/div[1]/div[2]/div/div[1]/div")))
            clipboard_button.click()


            clipboard_text= pyperclip.paste()
            print(clipboard_text)


            text_line = clipboard_text.splitlines()

            df_company.loc[idx,'Name_search'] = text_line[0].strip()
            df_company.loc[idx,'Address_search'] = text_line[1].strip()

            if(len(text_line) == 3):
                 df_company.loc[idx,'Tax_code_search'] = text_line[2].strip()
            else:
                df_company.loc[idx,'Tax_code_search'] = 'None'
           
            df_company.loc[idx,'Industry'] = driver.find_element(By.XPATH,  "//*[@id='chart-content']/div/div/div[1]/div[1]/div[2]/div/div[2]/ul/li[1]/span").text.strip()
            df_company.loc[idx,'URLs'] = driver.current_url

            
            # Get back to previous page
            driver.back()
    
    except NoSuchElementException as exception:

        df_company.loc[idx,'Exception'] = "No information found"
        continue

    finally:

        df_company.to_excel(r"C:\Users\ADMIN\Documents\Aufinia\Algieria\Algieria_Result_Euro.xlsx",sheet_name='Sheet1') 

print(df_company)
driver.close()


    

