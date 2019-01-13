import sys
import glob
import os
import time
import random
import json
import multiprocessing as mp
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Modified by User
OUTPUTFOLDER = "./folder"

# Number of restaurants each process will handle in parallel
INSTANCESIZE = 5

# Number of the process that can run in parallel
PROCESS_NUMBER = 8

# Wait time
LOAD_TIME = 2

# Retries in with in sub modules. Mostly in case of element load time and stuff
RETRIES = 2

# The retry instance on the sub modules. Wrapper around the sub modules retries. Loads a new scope completely as it restarts the sub module process
MAX_MAIN_RETRIES = 2


class Restaurant:
    """
        Contains the details of a restaurant.
    """
    def __init__(self, place_id):
        self.resturant = {
            "reviews": [],
            "id": place_id,
            "amenities": dict()
        }

    def add_amenities(self, key, a_list):
        self.resturant["amenities"][key] = a_list

    def add_reviews(self, stars, text, post_date):
        self.resturant["reviews"].append({"stars": stars, "text": text, "date": post_date})


class MapMiner:
    """
        The web crawler code that mines the data
    """

    @classmethod
    def get_driver(cls):
        """
        Fetch the Web driver of chrome or yelp at random to counter bot detection
        :return: Selenium driver object
        """
        choice = random.randint(0, 1)
        if choice > 0:
            chrome_options = webdriver.ChromeOptions()
            # Code for incognito mode
            chrome_options.add_argument("--incognito")
            # prefs = {"profile.managed_default_content_settings.images": 2}
            # chrome_options.add_experimental_option("prefs", prefs)
            # Load the browser
            driver = webdriver.Chrome("./chromedriver.exe", chrome_options=chrome_options)
        else:
            fp = webdriver.FirefoxProfile()
            fp.set_preference("browser.privatebrowsing.autostart", True)
            # fp.set_preference('permissions.default.image', 2)
            # fp.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
            driver = webdriver.Firefox(executable_path="./geckodriver", firefox_profile=fp)
        driver.delete_all_cookies()
        return driver

    @classmethod
    def page_init(cls, driver, location):
        # URL of the site
        driver.get(location["url"])

        ##
        # First check to see if the page loaded
        ##
        flag = 1
        while flag in range(10):
            try:
                # Map to get loaded
                WebDriverWait(driver, 5).until(
                    ec.presence_of_element_located((By.ID, "find_desc"))
                )
                flag = -1
            except Exception as ex:
                print("Failed to find the search input element. Page taking time to load. Retrying again "
                      + str(flag + 1) + "th time")
                flag += 1
                driver.quit()
                time.sleep(10 - random.randint(5, 8) * flag)

                driver = cls.get_driver()
                driver.get(location["url"])

        if flag > RETRIES:
            print("Error: Quiting for " + location["name"] + ". Please check your internet connection")
            raise Exception("Error: Quiting for " + location["name"] + ". Please check your internet "
                                                                       "connection. Exceeded max retries")

    @classmethod
    def get_amenities(cls, driver, restaurant, log, name):
        ##
        # Amenities Mining
        ##
        try:
            # Fetch all the amenities section
            ywidget = driver.find_elements_by_class_name("ywidget")
            for potential_amenity in ywidget:
                try:
                    if "business" in potential_amenity.find_element_by_tag_name("h3").text.lower():
                        amenities_containers = potential_amenity.find_elements_by_tag_name("dl")
                        for amenity_pair in amenities_containers:
                            restaurant.add_amenities(amenity_pair.find_element_by_tag_name("dt").text,
                                                     amenity_pair.find_element_by_tag_name("dd").text)
                        break
                except Exception as ex:
                    pass

        except Exception as ex:
            print(str(ex))
            print("Skipping Amenities for this resturant " + name)
            log.write("Skipping Amenities for this resturant " + name + ". Error: " + str(ex) + "\n")

    @classmethod
    def get_reviews(cls, driver, restaurant):
        while True:
            reviews_list = driver.find_elements_by_class_name("review-content")

            ran = random.randint(5, 10)

            # Setup the initial environment
            curr = 0
            while len(reviews_list) > curr and curr < 201 + ran:
                posted_date = reviews_list[curr].find_element_by_class_name("rating-qualifier").text
                stars = reviews_list[curr].find_element_by_class_name("i-stars").get_attribute("title").split(" ")[0]
                text = reviews_list[curr].find_element_by_tag_name("p").text
                restaurant.add_reviews(stars, text, posted_date)

                curr += 1

            try:
                pagination = driver.find_element_by_class_name("pagination-links")
                time.sleep(random.randint(1, 2))
                pagination.find_element_by_class_name("icon--24-chevron-right").click()
                time.sleep(LOAD_TIME)
            except Exception as ex:
                print("Either no more reviews or error")
                print(str(ex))
                break

        driver.get("www.google.com")

    @classmethod
    def scrap_data(cls, places, start, end, job, output):
        restaurant_list = []
        driver = None

        # Open Files to write data
        log = open(os.path.join(OUTPUTFOLDER, "Log" + "_subprocess-id" + str(job) + "_" + str(start) + "_" + str(end) +
                                ".txt"), "w+")
        output_file_path = os.path.join(OUTPUTFOLDER, "output" + "_subprocess-id" + str(job) + "_" + str(start) +
                                        "_" + str(end) + ".json")

        if end > len(places):
            end = len(places)

        indexing = -1
        try:
            for location in places[start: end]:
                if driver is None:
                    driver = cls.get_driver()

                indexing += 1
                print("Job Id: " + str(job) + " Initializing page for " + location["id"])
                restaurant = Restaurant(location["id"])

                flag = 0
                while flag < random.randint(1, MAX_MAIN_RETRIES):
                    try:
                        cls.page_init(driver, location)
                        print("Job Id: " + str(job) + " Amenities for " + location["id"])
                        cls.get_amenities(driver, restaurant, log, location["id"])
                        break
                    except Exception as ex:
                        driver.quit()
                        time.sleep(random.randint(1, 2))
                        driver = cls.get_driver()
                        cls.page_init(driver, location)
                        flag += 1

                ##
                # Review Mining
                ##
                flag = 0
                while flag < random.randint(1, MAX_MAIN_RETRIES):
                    try:
                        cls.get_reviews(driver, restaurant)
                        break
                    except Exception as ex:
                        driver.quit()
                        time.sleep(random.randint(0, 1))
                        driver = cls.get_driver()
                        cls.page_init(driver, location)
                        flag += 1

                # Finally append the restaurant information
                restaurant_list.append(restaurant.resturant)

                # Immediate result writing overwrite of the file
                output_file = open(output_file_path, "w")
                json.dump(restaurant_list, output_file)
                output_file.close()

                # Random check
                if random.randint(0, 1) > 0:
                    time.sleep(random.randint(0, 1))
                    driver.quit()
                    time.sleep(random.randint(0, 1))

            output.put("Success for process " + str(job) + " for indexes " + str(start) + " till " + str(end))
        except Exception as ex:
            print(ex)
            log.write(str(ex) + "\n")
            output.put(ex)
        finally:
            output_file = open(
                os.path.join(OUTPUTFOLDER,
                             "output" + "_subprocess-id" + str(job) + "_" + str(start) + "_" + str(end) + ".json"),
                "w")
            json.dump(restaurant_list, output_file)
            output_file.close()
            log.close()
            try:
                driver.close()
            except Exception as ex:
                pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Need the input folder")
        exit(1)

    try:
        places_list = []
        for f in glob.glob(sys.argv[1] + "/*.json"):
            #with open(f, 'rb') as infile:
            #    a = json.load(infile)
            #    places_list.extend(a)
            with open(f, encoding="utf8") as json_file:
                places_list.extend(json.load(json_file))

    except Exception as ex:
        print("Exception in reading the file. Exception: " + str(ex))
        exit(1)

    # Output Queue
    output = mp.Queue()
    start = 0

    print(len(places_list))

    while True:
        # Setup threads
        processes = [mp.Process(target=MapMiner.scrap_data,
                                args=(places_list, INSTANCESIZE * index + start,
                                      (INSTANCESIZE * (index+1)) + start, index, output))
                     for index in range(int(PROCESS_NUMBER))]

        # Run processes
        for i, p in enumerate(processes):
            if start + INSTANCESIZE * i <= len(places_list):
                p.start()
                time.sleep(2)
            else:
                # Remove the extra processes in reverse order
                for j in reversed(range(i, len(processes))):
                    processes.pop(j)
                break

        # Exit the completed processes
        for p in processes:
            p.join()

        # Get process results from the output queue
        results = [output.get() for p in processes]

        print(results)

        #time.sleep(random.randint(1, 2))

        start += PROCESS_NUMBER * INSTANCESIZE
        print("New Start = " + str(start))
        if start > len(places_list):
            print("Exiting")
            break