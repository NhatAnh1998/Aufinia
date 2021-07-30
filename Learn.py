from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en-US"})
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    return webdriver.Chrome(executable_path='C:/Users/ADMIN/Downloads/ChromeDriver/chromedriver.exe', options=chrome_options)

driver = create_driver()
driver.get('https://www.techwithtim.net/')

search_bar = driver.find_element_by_name("s")
search_bar.clear()
search_bar.send_keys("python", Keys.RETURN)

# dùng trong TH các element lồng vào nhau (theo DOM => element cha -> element con)
try:
    main = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "main"))
    )
    
    articles = main.find_elements_by_tag_name("article")

    for article in articles:
        header = article.find_element_by_class_name("entry-summary")
        print(header.text)


finally:
    driver.quit()



