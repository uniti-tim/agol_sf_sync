def enable_exports(os, time, id):
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys

    driver = webdriver.Chrome(executable_path=r'C:\chromedriver.exe')
    driver.get("http://uniti.maps.arcgis.com/home/item.html?id="+ id +"#settings")
    time.sleep(8)

    login = driver.find_element_by_id("oAuthFrame")
    if login:
        print("Login is needed - Going to Login")
        driver.switch_to.frame("oAuthFrame")
        driver.find_element_by_id('user_username').send_keys(os.environ.get("AGOL_USER"))
        driver.find_element_by_id('user_password').send_keys(os.environ.get("AGOL_PASS"))
        driver.find_element_by_name('authorize').click()
        time.sleep(15)
        driver.switch_to.default_content();

    print("enabling exports")
    driver.find_element_by_xpath('//*[@id="uniqName_9_2"]/fieldset[5]/label/input').click()
    driver.find_element_by_xpath('//*[@id="uniqName_15_1"]/div[2]/section/button[2]').click()
    print("Saving settings")
    time.sleep(6)
    driver.close()
    driver.quit()
    print("Done updating export setting")
