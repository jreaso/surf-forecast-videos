import json
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc  # avoids cloudflare bot preventions


def fetch_rewind_links(rewind_extensions: dict, headless=True, num_days: int = 2) -> list:
    """
    Semi-automated (may require CAPTCHA solved by human) Selenium based web scraper to log in to surfline and parse the
    rewinds page for a surf spot, saving the urls to the surf_cam_videos on the cdn server.

    :param rewind_extensions: dict of extensions on the base url for the rewind clips page for surf spots.
    :param headless: whether to run browser headless or not.
    :param num_days: How many pages to scrape from (how many days back). Max is 5.
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
    json_data_file_path = os.path.join("..", "data", "config.json")  # Build the path to config.json
    # Load the credential configuration from the JSON file
    with open(json_data_file_path, 'r') as config_file:
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

    # Initialise empty dictionary to store lists of rewind clips for each spot
    rewind_clip_urls_all = {}

    # Redirect to Rewinds Page
    for cam, spot_rewind_extension in rewind_extensions.items():
        rewind_url = f'https://www.surfline.com/surf-cams/{spot_rewind_extension}'
        print(f"Navigating to {rewind_url}")
        driver.get(rewind_url)

        # Wait until page loaded
        try:
            wait.until(EC.presence_of_element_located((By.ID, "sl-rewind-player")))
        except:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "CameraClips_cameraClips__KFOUH")))

        # Store Clip URLs From Page
        rewind_clip_urls = []  # initialize an empty list to append to

        # Cycle through days
        for i in range(num_days):
            # Find and click the dropdown div
            dropdown_div = driver.find_element(By.ID, "sl-cam-rewind-date")
            dropdown_div.click()

            # Wait until options have loaded and click indexed option
            dropdown_ul = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "MuiMenu-list")))
            dropdown_options = dropdown_ul.find_elements(By.TAG_NAME, "li")
            option = dropdown_options[i]
            option.click()

            try:
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

                rewind_clip_urls_all[cam] = rewind_clip_urls
            except:
                print("Error loading page")

    driver.quit()

    return rewind_clip_urls_all
