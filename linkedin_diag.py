from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time


def make_driver_local():
    opts = Options()
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(45)
    return driver


def run():
    driver = make_driver_local()
    try:
        url = "https://www.linkedin.com/jobs/search/?keywords=data%20scientist&location=Toronto%2C%20ON"
        print(f"Loading: {url}")
        driver.get(url)
        time.sleep(6)

        selectors = [
            ".job-card-container",
            "div.job-card-container",
            "li.jobs-search-results__list-item",
            "div.jobs-search-result-card",
            "div.job-card-list__entity-lockup",
            "div.base-card",
            "a.job-card-list__title",
            "a.job-card-container__link",
            "h3.base-search-card__title",
            "ul.jobs-search__results-list li",
            ".jobs-search-results__list li",
            ".jobs-search-results-list",
        ]

        for sel in selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                print(f"{sel} -> {len(els)}")
            except Exception as e:
                print(f"{sel} -> ERROR: {e}")

        ua = driver.execute_script("return navigator.userAgent;")
        print("User-Agent:", ua)

        ts = time.strftime("%Y%m%d-%H%M%S")
        fname_html = f"linkedin_diag_{ts}.html"
        fname_png = f"linkedin_diag_{ts}.png"
        try:
            with open(fname_html, "w", encoding="utf-8") as fh:
                fh.write("<!-- URL: " + driver.current_url + " -->\n<!-- UA: " + ua + " -->\n" + driver.page_source)
            print("Wrote HTML snapshot:", fname_html)
        except Exception as e:
            print("Failed to write HTML snapshot:", e)

        try:
            driver.save_screenshot(fname_png)
            print("Wrote screenshot:", fname_png)
        except Exception as e:
            print("Failed to write screenshot:", e)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == '__main__':
    run()
