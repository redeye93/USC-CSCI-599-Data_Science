import sys
import json
import os
import time
import math
import json
import multiprocessing as mp
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Folder path where the json data will be dumped
OUTPUTFOLDER = "C:\\Users\\utkar\\Downloads\\UIscrapper\\Google\\15th Oct\\data\\iteration4_1"

# Instance size is the number of restaurants each process will handle parallely
INSTANCESIZE = 10

# Number of process that can run parallel at a given point in time
PROCESS_NUMBER = 6

# Wait time to be given to let the element to load/process after the initiation of an action/event
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
        self.restaurant = {
            "popularTimes": dict(),
            "reviews": [],
            "place_id": place_id,
            "amenities": dict()
        }

    def add_popular_times(self, day, time_list):
        self.restaurant["popularTimes"][day] = time_list

    def add_amenities(self, key, a_list):
        self.restaurant["amenities"][key] = a_list

    def add_reviews(self, stars, text, posted):
        self.restaurant["reviews"].append({"stars": stars, "text": text, "time": posted})

    @classmethod
    def get_days(cls):
        return ["Sundays", "Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays"]


class MapMiner:
    """
    The web crawler code that mines the data
    """

    @classmethod
    def get_driver(cls):
        """
        Call to the function to return a new selenium driver. Code for chrome and firefox present
        :return: Selenium driver object
        """
        #chrome_options = webdriver.ChromeOptions()
        # Code for incognito mode
        #chrome_options.add_argument("--incognito")
        # Load the browser
        #driver = webdriver.Chrome("./chromedriver.exe", chrome_options=chrome_options)

        # Code for mozilla
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.privatebrowsing.autostart", True)
        fp.set_preference('permissions.default.image', 2)
        fp.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
        driver = webdriver.Firefox(executable_path="./geckodriver", firefox_profile=fp)
        return driver

    @classmethod
    def page_init(cls, driver, url, place_id):
        """
        Initializes the selenium driver object to the initial state
        :param driver: Selenium driver object
        :param url: URL of the restaurant for google maps
        :param place_id: Unique id - generally google place id
        :return: null
        """
        while True:
            try:
                # URL of the site
                driver.get(url)

                ##
                # First check to see if the page loaded
                ##
                flag = 1
                while flag in range(10):
                    try:
                        # Map to get loaded
                        WebDriverWait(driver, 5).until(
                            ec.presence_of_element_located((By.ID, "searchboxinput"))
                        )
                        flag = -1
                    except Exception as ex:
                        print("Failed to find the search input element. Page taking time to load. Retrying again "
                              + str(flag + 1) + "th time")
                        flag += 1
                        driver.quit()
                        time.sleep(2)
                        driver = cls.get_driver()
                        driver.get(url)

                if flag > RETRIES:
                    print("Error: Quiting for " + place_id + ". Please check your internet connection")
                    raise Exception("Error: Quiting for " + place_id + ". Please check your internet "
                                                                               "connection. Exceeded max retries")
                else:
                    return
            except Exception as ex:
                print("Exception encountered " + str(ex))
                print("Go to sleep for 10 sec")
                time.sleep(6)
                driver.close()
                driver = cls.get_driver()

    @classmethod
    def get_time(cls, driver, restaurant, name):
        """
        Responsible for fetching the popular times of the restaurant, if present
        :param driver: Selenium driver object
        :param restaurant: Restaurant class object
        :param name:  Name of the place
        :return:
        """
        ##
        # Second check to see if days part is loaded
        ##
        flag = 1
        while flag in range(2):
            try:
                # element to become interactable
                time.sleep(LOAD_TIME)
                driver.execute_script(
                    "document.querySelectorAll('div.section-popular-times-select')[0].scrollIntoView()")

                # Dynamic load of the days part
                day_dropdown = driver.find_element_by_class_name("section-popular-times-select")
                time.sleep(1)
                day_dropdown.click()

                # Wait for content to be loaded
                time.sleep(1)
                flag = -1
            except Exception as ex:
                print("Days drop down still not interactable. Checking for " + str(flag) + "th time.")
                flag += 1

        if flag > RETRIES:
            print("Day drop-down was not interactable. Please check your internet connection.")
            raise Exception("Error: Days drop down still not interactable. Quiting for " + name
                            + ". Please check your internet connection. Exceeded max retries")

        # Find the Popular time bars container
        time_bars_container = driver.find_elements_by_class_name("section-popular-times-graph")

        for i in range(len(time_bars_container)):
            time_bars = time_bars_container[i].find_elements_by_class_name("section-popular-times-bar")
            day = Restaurant.get_days()[i]

            time_list = []
            for j in range(len(time_bars)):
                time_list.append(time_bars[j].get_attribute("aria-label"))

            restaurant.add_popular_times(day, time_list)

    @classmethod
    def get_amenities(cls, driver, restaurant, log, places_id):
        """
        Scraps all the amenities from the text table format, if present. But it first needs to go to that page.
        :param driver:
        :param restaurant:
        :param log:
        :param places_id:
        :return:
        """
        ##
        # Amenities Mining
        ##
        try:
            flag = 1
            while flag in range(6):
                try:
                    driver.execute_script(
                        "document.querySelectorAll('button.section-editorial-button')[0].scrollIntoView()")
                    time.sleep(1)
                    amenities_details = driver.find_element_by_class_name("section-editorial-button")
                    amenities_details.click()

                    # Wait for content to be loaded
                    time.sleep(1)
                    flag = -1
                except Exception as ex:
                    time.sleep(1)
                    print("Failed to click the amenities outer button for " + str(flag) +
                          "th time. Try with inner div")

                    # Try with inner div
                    try:
                        time.sleep(1)
                        amenities_details = driver.find_element_by_class_name("section-editorial-attributes")
                        amenities_details.click()

                        # Wait for content to be loaded
                        time.sleep(1)
                        flag = -1
                    except Exception as ex:
                        time.sleep(1)
                        print("Failed to click the amenities inner div for " + str(flag) + "th time")
                        flag += 1

            if flag > 5:
                raise Exception("Failed to mine amenities. Try again.")

            # Fetch all the amenities section
            heading_containers_list = driver.find_elements_by_class_name("section-attribute-group")
            for heading_container in heading_containers_list:
                a_list = []

                heading = heading_container.find_element_by_class_name("section-attribute-group-title").text
                amenities_names_elements = heading_container.find_elements_by_class_name(
                    "section-attribute-group-item")
                for amenity_element in amenities_names_elements:
                    a_list.append(amenity_element.find_element_by_tag_name("span").text)

                restaurant.add_amenities(heading, a_list)

        except Exception as ex:
            print(str(ex))
            print("Skipping Amenities for this resturant " + places_id)
            log.write("Skipping Amenities for this resturant " + places_id + ". Error: " + str(ex) + "\n")

    @classmethod
    def get_reviews(cls, driver, restaurant):
        """
        Scraps all the reviews form the UI. However canput a limit on them buy placing a condition in the while loop
        :param driver:
        :param restaurant:
        :return:
        """
        # Click the link for more reviews
        driver.execute_script(
            "document.querySelectorAll('button.section-reviewchart-numreviews')[0].scrollIntoView()")

        driver.find_element_by_class_name("section-reviewchart-numreviews").click()

        time.sleep(LOAD_TIME)

        # Setup the initial environment
        curr = 0
        reviews_container = driver.find_element_by_class_name("section-listbox")
        # Fetch out the reviews
        reviews = reviews_container.find_elements_by_class_name("section-review")

        while len(reviews) > curr:
            driver.execute_script(
                "document.querySelectorAll('div.section-review')[" + str(curr) + "].scrollIntoView()")
            stars = len(reviews[curr].find_element_by_class_name(
                "section-review-stars").find_elements_by_class_name("section-review-star-active"))

            try:
                reviews[curr].find_element_by_class_name("section-expand-review").click()
            except Exception as ex:
                # No more option present
                pass
            text = reviews[curr].find_element_by_class_name("section-review-text").text
            posted_on = reviews[curr].find_element_by_class_name("section-review-publish-date").text

            restaurant.add_reviews(stars, text, posted_on)

            curr += 1
            if curr % 8 == 0:
                time.sleep(LOAD_TIME)

            # Refresh the reference of the reviews
            reviews_container = driver.find_element_by_class_name("section-listbox")
            # Fetch out the reviews
            reviews = reviews_container.find_elements_by_class_name("section-review")

    @classmethod
    def scrap_data(cls, places_id_list, places_dict, start, end, job, output):
        """
        The main function to trigger the scrapper
        :param places_id_list: List of place ids to be scrapped.
        :param places_dict: Place details objects with keys as place ids
        :param start: In case of custom start with respect to the place id list
        :param end: In case of custom end with respect to the place id list
        :param job: Job number in each cycle of triggering n parallel jobs
        :param output: output queue to push the final result to the parent process
        :return: null
        """
        restaurant_list = []
        driver = cls.get_driver()

        log = open(os.path.join(OUTPUTFOLDER, "Log" + "_subprocess-id" + str(job) + "_" + str(start) + "_" + str(end) + ".txt"),
                   "w+")

        if end > len(places_id_list):
            end = len(places_id_list)

        indexing = -1
        try:
            for places_id in places_id_list[start:end]:
                print("Job Id: " + str(job) + " Initializing page for " + places_id)
                indexing += 1
                restaurant = Restaurant(places_id)

                flag = 1
                while flag < MAX_MAIN_RETRIES:
                    try:
                        cls.page_init(driver, places_dict[places_id], places_id)
                        print("Job Id: " + str(job) + " Time fetch " + places_id)
                        cls.get_time(driver, restaurant, places_dict[places_id])
                        break
                    except Exception as ex:
                        driver.close()
                        time.sleep(2)
                        driver = cls.get_driver()
                        flag += 1

                flag = 1
                while flag < MAX_MAIN_RETRIES:
                    try:
                        print("Job Id: " + str(job) + " Amenities for " + places_id)
                        cls.get_amenities(driver, restaurant, log, places_id)
                        break
                    except Exception as ex:
                        driver.close()
                        driver = cls.get_driver()
                        cls.page_init(driver, places_dict[places_id], places_id)
                        flag += 1

                ##
                # Review Mining
                ##
                try:
                    print("Job Id: " + str(job) + " Reviews for " + places_id)
                    # First go back to the main page
                    driver.execute_script(
                        "document.querySelectorAll('button.section-header-back-button')[0].scrollIntoView()")
                    time.sleep(1)
                    driver.find_element_by_class_name("section-header-back-button").click()

                    # Wait for content to be loaded
                    time.sleep(3)
                except Exception as ex:
                    time.sleep(1)
                    print("Failed to go back to general page for review. will start over again for " + places_id)
                    driver.close()
                    driver = cls.get_driver()
                    cls.page_init(driver, places_dict[places_id], places_id)

                flag = 1
                while flag < MAX_MAIN_RETRIES:
                    try:
                        cls.get_reviews(driver, restaurant)
                        break
                    except Exception as ex:
                        driver.close()
                        driver = cls.get_driver()
                        time.sleep(2)
                        cls.page_init(driver, places_dict[places_id], places_id)
                        flag += 1

                # Finally append the restaurant information
                restaurant_list.append(restaurant.restaurant)

                driver.close()
                time.sleep(2)
                driver = cls.get_driver()

            output.put("Success for process " + str(job) + " for indexes " + str(start) + " till " + str(end))
        except Exception as ex:
            print(ex)
            log.write(str(ex) + "\n")
            log.write("Failed for place ids - " + str(places_id_list[start + indexing: end]))
            output.put(ex)

            fail_dict = dict()
            try:
                with open(os.path.join(OUTPUTFOLDER,
                             "fail" + "_subprocess-id" + str(job) + ".json"), encoding="utf8") as json_file:
                    fail_dict = json.load(json_file)
            except Exception as ex:
                pass

            for j in range(start + indexing, end):
                fail_dict[places_id_list[j]] = places_dict[places_id_list[j]]

            fail_file = open(
                os.path.join(OUTPUTFOLDER, "fail" + "_subprocess-id" + str(job) + ".json"), "w+")
            json.dump(fail_dict, fail_file)
            fail_file.close()
        finally:
            output_file = open(
                os.path.join(OUTPUTFOLDER, "output" + "_subprocess-id" + str(job) + "_" + str(start) + "_" + str(end) + ".json"),
                "w+")
            json.dump(restaurant_list, output_file)
            output_file.close()
            log.close()
            try:
                driver.close()
            except Exception as ex:
                pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Need the input file")
        exit(1)

    if not Path(sys.argv[1]).is_file():
        print("File path doesn't exist")
        exit(1)
    try:
        with open(sys.argv[1], encoding="utf8") as json_file:
            places_dict = json.load(json_file)
    except Exception as ex:
        print("Exception in reading the file. Exception: " + str(ex))
        exit(1)

    # Output Queue
    output = mp.Queue()
    places_id_list = list(places_dict.keys())
    start = 0

    while True:
        # Setup threads
        processes = [mp.Process(target=MapMiner.scrap_data,
                                args=(places_id_list, places_dict, start + INSTANCESIZE * index,
                                      start + INSTANCESIZE * (index+1), index, output))
                     for index in range(PROCESS_NUMBER)]

        # Run processes
        for i, p in enumerate(processes):
            if start + INSTANCESIZE * i <= len(places_id_list):
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

        start += PROCESS_NUMBER * INSTANCESIZE

        if start > len(places_id_list):
            print("Exiting")
            break
