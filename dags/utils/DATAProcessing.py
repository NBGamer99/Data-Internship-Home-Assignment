from math import e
import re
import pandas as pd
import os
import json
from bs4 import BeautifulSoup
import html
from datetime import datetime
from airflow.exceptions import AirflowNotFoundException
from utils.DBManager import DatabaseManager

STAGING_EXTRACTED = "./staging/extracted"
STAGING_TRANSFORMED = "./staging/transformed"


# ====================================== Helper functions =============================================

def get_parsed_html(data):
    # The code snippet is a helper function that takes a string of HTML content and returns a
    # BeautifulSoup object.
    html_parsed = html.unescape(data.get("description", ""))
    soup = BeautifulSoup(html_parsed, "html.parser")
    return soup


def format_date(date_str):
    # Parse the provided date string
    original_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # Convert the date to a new format, for instance, YYYY-MM-DD HH:MM:SS
    transformed_date = original_date.strftime("%Y-%m-%d %H:%M:%S")
    return transformed_date

# ====================================== End Helper Functions ======================================

# ======================================= Extract Data from CSV =======================================


def extract_csv(path):
    df = pd.read_csv(path)
    for index, row in df.iterrows():
        print(f"Extracting data... {index + 1}")
        context_str = str(row["context"])
        # Skip the row if the context is empty
        if context_str.strip() == "{}":
            continue
        with open(f"{STAGING_EXTRACTED}/{index + 1}.txt", "w") as file:
            file.write(context_str)


# ======================================= End Extract Data from CSV =======================================

# ======================================= Get Job Info =======================================


def get_job_info(data):
    soup = get_parsed_html(data)
    job_title = data.get("title") if data.get("title", "") else extract_job_title(soup)
    industry = data.get("industry", "")
    employment_type = data.get("employmentType", "")
    description = get_clean_description(soup)
    date_posted = format_date(data.get("datePosted", ""))

    return {
        "job_title": job_title,
        "industry": industry,
        "description": description,
        "employment_type": employment_type,
        "date_posted": date_posted,
    }


def extract_job_title(soup):
    job_title = soup.find_all(string=re.compile("Job Title:"))
    return (
        job_title[0].replace("Job Title: ", "").strip() if (len(job_title) != 0) else ""
    )


def get_clean_description(soup):
    return soup.get_text()


# ======================================= End Get Job Info =======================================

# ======================================= Get Company Info =======================================


def get_company_info(data):
    name = data.get("hiringOrganization", "").get("name")
    link = data.get("hiringOrganization", "").get("sameAs")
    return {
        "name": name,
        "link": link,
    }


# ======================================= End Get Company Info =======================================

# ======================================= Get Education Info =======================================


def extract_education_req(soup):
    education_requirements = soup.find_all("ul", recursive=True)
    education_requirements = [
        li.get_text(strip=True)
        for ul in education_requirements
        for li in ul.find_all("li")
    ]
    education_keywords = [
        "B.S",
        "M.S",
        "master",
        "bachelor",
        "phd",
        "degree",
        "university",
        "college",
        "academic",
    ]
    filtered_results = [
        result
        for result in education_requirements
        if any(keyword in result.lower() for keyword in education_keywords)
    ]
    return filtered_results[0] if filtered_results else ""


def get_education_info(data):
    soup = get_parsed_html(data)
    education_requirements = data.get("educationRequirements").get("credentialCategory", "") if data.get("educationRequirements", "") else extract_education_req(soup)
    return {
        "required_credential": education_requirements,
    }


# ======================================= End Get Education Info =======================================

# ======================================= Get Estimated Salary =======================================


def get_min_max_salary(salary_range):
    salary_text = " ".join(salary_range)
    k_format = re.findall(r"\$(\d+\.?\d*)-?(\d+\.?\d*)?k", salary_text, re.IGNORECASE)
    all_numbers = re.findall(
        r"\$(\d+\.?\d*)-?(\d+\.?\d*)?k|\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)|\b(\d+)\b",
        salary_text,
        re.IGNORECASE,
    )
    numbers = ""

    if k_format:
        numbers = [
            int(float(number) * 1000) for pair in k_format for number in pair if number
        ]
    if all_numbers:
        numbers = [
            int(float(number))
            if "k" in number
            else int(number.replace(",", "").split(".")[0])
            for sublist in all_numbers
            for number in sublist
            if number
        ]
        if "week" not in salary_text.lower():
            numbers = [number * 1000 if number < 1000 else number for number in numbers]

    if numbers:
        min_salary = min(numbers)
        max_salary = max(numbers)
        return min_salary, max_salary
    else:
        return None


def get_salary_info(data):
    soup = get_parsed_html(data)

    min_value, max_value = "", ""
    if data.get("estimatedSalary"):
        min_value = data.get("estimatedSalary").get("value", {}).get("minValue")
        max_value = data.get("estimatedSalary").get("value", {}).get("maxValue")
    else:
        value = get_min_max_salary(extract_salary_range(soup))
        if value:
            min_value, max_value = value

    currency = (
        data.get("estimatedSalary").get("currency")
        if data.get("estimatedSalary", "")
        else ""
    )
    unit = (
        data.get("estimatedSalary").get("value").get("unitText")
        if data.get("estimatedSalary", "")
        else ""
    )

    return {
        "currency": currency,
        "min_value": min_value,
        "max_value": max_value,
        "unit": unit,
    }


def extract_salary_range(soup):
    salary_range = soup.find_all(string=re.compile("salary", re.IGNORECASE))
    return salary_range


# ======================================= End Get Estimated Salary =======================================

# ======================================= Get Location Info =======================================

def get_location_info(data):
    return {
        "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry"),
        "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality"),
        "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion"),
        "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode"),
        "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress"),
        "latitude": data.get("jobLocation", {}).get("latitude"),
        "longitude": data.get("jobLocation", {}).get("longitude"),
    }

# ======================================= End Get Location Info =======================================


# ======================================= Get Experience Info =======================================

def get_seniority_level(monthsOfExperience):
    if monthsOfExperience <= 12:
        return "Entry level"
    elif monthsOfExperience <= 60:
        return "Associate"
    elif monthsOfExperience <= 120:
        return "Mid-Senior level"
    else:
        return "Senior level"

def get_experience_info(data):
    months_of_experience = 0
    if data.get("experienceRequirements", {}) != "no requirements":
        months_of_experience = data.get("experienceRequirements", {}).get("experienceInPlaceOfEducation", 0)

    return{
        "months_of_experience" : months_of_experience,
        "seniority_level": get_seniority_level(months_of_experience)
    }

# ======================================= End Get Experience Info =======================================

# ======================================= Transform Data =======================================
def transform_schema(data):
    transformed_data = {
        "job": get_job_info(data),
        "company": get_company_info(data),
        "education": get_education_info(data),
        "experience": get_experience_info(data),
        "salary": get_salary_info(data),
        "location": get_location_info(data),
    }
    return transformed_data


def transform_extracted_data():
    file_names = os.listdir(STAGING_EXTRACTED)
    # Filter out files that don't follow the "number.txt" naming convention
    file_names = [
        file_name for file_name in file_names if file_name.split(".")[0].isdigit()
    ]
    # Sort the file names in descending order
    file_names = sorted(file_names, key=lambda x: int(x.split(".")[0]))
    for filename in file_names:
        if filename.endswith(".txt"):
            print(f"Transforming data... {filename}")
            input_file_path = os.path.join(STAGING_EXTRACTED, filename)
            output_file_path = os.path.join(
                STAGING_TRANSFORMED, f"{os.path.splitext(filename)[0]}.json"
            )

            # Check if the file is not empty
            if os.path.getsize(input_file_path) == 0:
                print(f"Warning: Skipping empty file: {filename}")
                continue
            try:
                with open(input_file_path, "r", encoding="utf-8") as file:
                    try:
                        json_data = json.loads(file.read())
                    except json.JSONDecodeError:
                        print(f"Error: Invalid JSON format in file: {filename}")
                        continue
            except AirflowNotFoundException:
                print("Couldn't open file in extracted folder")
                continue

            transformed_data = transform_schema(json_data)

            # Save the transformed data to a new JSON file in the output folder
            with open(output_file_path, "w", encoding="utf-8") as output_file:
                json.dump(transformed_data, output_file, ensure_ascii=False, indent=4)


# ======================================= End Transform Data =======================================


# ====================================== Loading to databas ========================================

def dict_to_tuple(data, job_id=None):
    if job_id is None:
        return (None, *data.values())
    return (None,job_id, *data.values())

def load_to_db():
    database_manager = DatabaseManager()

    job_rows = []
    company_rows = []
    education_rows = []
    experience_rows = []
    salary_rows = []
    location_rows = []

    file_names = os.listdir(STAGING_TRANSFORMED)
    # Filter out files that don't follow the "number.json" naming convention
    file_names = [
        file_name for file_name in file_names if file_name.split(".")[0].isdigit()
    ]
    # Sort the file names in descending order
    file_names = sorted(file_names, key=lambda x: int(x.split(".")[0]))
    for index, filename in enumerate(file_names):
        print(f"Loading data to database... {index + 1}")
        if filename.endswith(".json"):
            input_file_path = os.path.join(STAGING_TRANSFORMED, filename)
            try:
                with open(input_file_path) as file:
                    data = json.load(file)
            except AirflowNotFoundException:
                    print("Couldn't open file in transformed folder")
                    continue
            job_rows.append(dict_to_tuple(data["job"]))
            company_rows.append(dict_to_tuple(data["company"], index + 1))
            education_rows.append(dict_to_tuple(data["education"], index + 1))
            experience_rows.append(dict_to_tuple(data["experience"], index + 1))
            salary_rows.append(dict_to_tuple(data["salary"], index + 1))
            location_rows.append(dict_to_tuple(data["location"], index + 1))

    database_manager.insert_many(job_rows, "job")
    database_manager.insert_many(company_rows, "company")
    database_manager.insert_many(education_rows, "education")
    database_manager.insert_many(experience_rows, "experience")
    database_manager.insert_many(salary_rows, "salary")
    database_manager.insert_many(location_rows, "location")

# ====================================== End Loading to databas ========================================
