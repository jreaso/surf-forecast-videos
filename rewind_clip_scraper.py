import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import undetected_chromedriver as uc  # avoids cloudflare bot preventions


def fetch_rewind_links(spot_rewind_extensions: list, headless=False) -> list:
    """
    Semi-automated (may require CAPTCHA solved by human) Selenium based web scraper to log in to surfline and parse the
    rewinds page for a surf spot, saving the urls to the videos on the cdn server.

    :param spot_rewind_extensions: list of extensions on the base url for the rewind clips page for a certain surf spot.
    :param headless: whether to run browser headless or not.
    :return: rewind_clip_urls, a list of urls with links to the cdn server for downloading clips.
    """
    # Initialize Selenium Driver
    options = uc.ChromeOptions()
    if headless:
        # Headless Option may throw error for CAPTCHA
        options.headless = True
        options.add_argument('--headless')
    driver = uc.Chrome(options=options)

    wait = WebDriverWait(driver, 20)  # wait instance to wait for elements to load

    # Log In
    # Load the credential configuration from the JSON file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    email = config['email']
    password = config['password']

    login_url = 'https://www.surfline.com/sign-in'
    driver.get(login_url)

    # accept cookies pop up
    accept_button = wait.until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
    accept_button.click()
    wait.until(EC.invisibility_of_element_located((By.ID, "onetrust-accept-btn-handler")))

    # Locate and fill in the email and password fields
    email_field = wait.until(EC.presence_of_element_located((By.NAME, 'email')))
    password_field = driver.find_element(By.NAME, 'password')

    email_field.send_keys(email)
    password_field.send_keys(password)

    # Locate and click the login button
    login_button = driver.find_element(By.XPATH, '//button[normalize-space()="Sign in"]')
    login_button.click()

    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "NavigationBar_account__mC_o1")))  # wait until logged in
    print("Logged In")

    rewind_clip_urls_all = []

    # Redirect to Rewinds Page
    for spot_rewind_extension in spot_rewind_extensions:
        rewind_url = f'https://www.surfline.com/surf-cams/{spot_rewind_extension}'
        driver.get(rewind_url)

        wait.until(EC.presence_of_element_located((By.ID, "sl-rewind-player")))  # wait until page loaded

        # Store Clip URLs From Page
        rewind_clip_urls = []  # initialize an empty list to append to

        # Cycle Through Days
        select_element = driver.find_element(By.ID, 'sl-cam-rewind-date')  # Locate the <select> element

        # Create a Select object to interact with the <select> element
        select = Select(select_element)

        # Get the options before interacting
        option_values = [option.get_attribute("value") for option in select.options]

        for option_value in option_values:
            # Re-find the select element
            select_element = driver.find_element(By.ID, 'sl-cam-rewind-date')
            select = Select(select_element)

            select.select_by_value(option_value)  # select the option

            wait.until(EC.visibility_of_element_located((By.ID, "sl-rewind-player")))  # wait until page loaded

            # Cycle Through Clips
            rewind_clips_container = driver.find_elements(By.CLASS_NAME, "CameraClips_cameraClips__KFOUH")[0]
            rewind_clip_buttons = rewind_clips_container.find_elements(By.CLASS_NAME, "CameraClips_cameraClip__Wh_DL")

            # Loop over the clips and click the button for each clip
            for button in rewind_clip_buttons:
                button.click()

                # wait until link div is loaded
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "CamRewind_camRewindDownloadBar__hc24X")))

                # Select Link
                element = driver.find_element(By.CLASS_NAME, "CamRewind_camRewindDownloadBar__hc24X")
                link = element.find_element(By.TAG_NAME, "a").get_attribute("href")

                rewind_clip_urls.append(link)  # append link

                # wait until page loaded fully before moving on
                wait.until(EC.visibility_of_element_located((By.ID, "sl-rewind-player")))

            rewind_clip_urls_all.append(rewind_clip_urls)

    driver.quit()

    return rewind_clip_urls_all



