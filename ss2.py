from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import csv
import time

# --- Config ---
URL = "https://health-products.canada.ca/dpd-bdpp/index-eng.jsp"
CATEGORY = "Cancelled (Unreturned Annual)"
OUTPUT_CSV = "cancelled_unreturned_annual.csv"

driver = webdriver.Chrome()
wait = WebDriverWait(driver, 15)
driver.get(URL)


# --- Select category ---
select_element = wait.until(EC.presence_of_element_located((By.ID, "status")))
select = Select(select_element)
select.deselect_all()            # <- Deselect everything first
select.select_by_visible_text(CATEGORY)
print(f"🔎 Scraping category: {CATEGORY}")


# --- Click search ---
search_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='Search']")))
search_button.click()
print("✅ Search button clicked successfully.")

all_data = []

# --- Scrape headers ---
table = wait.until(EC.presence_of_element_located((By.ID, "results")))
headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
all_data.append(headers)

# --- Pagination loop ---
while True:
    # Wait for table rows
    table = wait.until(EC.presence_of_element_located((By.ID, "results")))
    rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#results tbody tr")))

    for row in rows:
        cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
        if cols:
            all_data.append(cols)

    # Check for next page
    next_button = driver.find_element(By.ID, "results_next")
    if "disabled" in next_button.get_attribute("class"):
        break  # last page
    driver.execute_script("arguments[0].click();", next_button)
    # Wait for new page
    wait.until(EC.staleness_of(rows[0]))
    time.sleep(1)  # small buffer

# --- Save to CSV ---
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(all_data)

print(f"✅ Scraping completed! Total rows: {len(all_data)-1} (excluding headers)")

driver.quit()
