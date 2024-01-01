from enum import Enum
import requests
from bs4 import BeautifulSoup
from threading import Lock
import time
from requests.exceptions import ProxyError
import re
from urllib.parse import urlparse, urlunparse
import datetime
from pprint import pprint
import pandas as pd
from tqdm import tqdm
import random

class JobType(Enum):
    FULL_TIME = (
        "fulltime",
        "períodointegral",
        "estágio/trainee",
        "cunormăîntreagă",
        "tiempocompleto",
        "vollzeit",
        "voltijds",
        "tempointegral",
        "全职",
        "plnýúvazek",
        "fuldtid",
        "دوامكامل",
        "kokopäivätyö",
        "tempsplein",
        "vollzeit",
        "πλήρηςαπασχόληση",
        "teljesmunkaidő",
        "tempopieno",
        "tempsplein",
        "heltid",
        "jornadacompleta",
        "pełnyetat",
        "정규직",
        "100%",
        "全職",
        "งานประจำ",
        "tamzamanlı",
        "повназайнятість",
        "toànthờigian",
    )
    PART_TIME = ("parttime", "teilzeit", "částečnýúvazek", "deltid")
    CONTRACT = ("contract", "contractor")
    TEMPORARY = ("temporary",)
    INTERNSHIP = (
        "internship",
        "prácticas",
        "ojt(onthejobtraining)",
        "praktikum",
        "praktik",
    )

    PER_DIEM = ("perdiem",)
    NIGHTS = ("nights",)
    OTHER = ("other",)
    SUMMER = ("summer",)
    VOLUNTEER = ("volunteer",)

def job_type_code(job_type_enum):
    mapping = {
        JobType.FULL_TIME: "F",
        JobType.PART_TIME: "P",
        JobType.INTERNSHIP: "I",
        JobType.CONTRACT: "C",
        JobType.TEMPORARY: "T",
    }
    return mapping.get(job_type_enum, "")
seen_urls = set()
MAX_RETRIES = 5
DELAY = 10

def get_DELAY():
    return random.randrange(5,20)

job_list = []
results_wanted = 200
search_term = "software developer,software engineer"
location = "India"
distance = 100
is_remote = None
job_type = "fulltime"
offset = 25
easy_apply = None
url = "https://www.linkedin.com"
page = offset // 25 + 25 if offset else 0
proxy = (lambda p: {"http": p, "https": p} if p else None)(None)
emails = []

url_lock = Lock()

def get_enum_from_job_type(job_type_str):
    """
    Given a string, returns the corresponding JobType enum member if a match is found.
    """
    res = None
    for job_type in JobType:
        if job_type_str in job_type.value:
            res = job_type
    return res


def extract_emails_from_text(text: str) -> list[str] | None:
    if not text:
        return None
    email_regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    possible_emails = email_regex.findall(text)
    emails = []
    for e in possible_emails:
        try:
            start = e.index('.com')
            end = start+4
            emails.append(e[:end])
            continue
        except ValueError:
            pass

        try:
            start = e.index('.in')
            end = start+3
            emails.append(e[:end])
            continue
        except ValueError:
            pass

        try:
            start = e.index('.io')
            end = start+3
            emails.append(e[:end])
            continue
        except ValueError:
            pass

        try:
            start = e.index('.co')
            end = start+3
            emails.append(e[:end])
            continue
        except ValueError:
            pass

        emails.append(e)
    return emails


def get_job_description(job_page_url):
        try:
            response = requests.get(job_page_url, timeout=5, proxies=proxy)
            response.raise_for_status()
        except requests.HTTPError as e:
            if hasattr(e, "response") and e.response is not None:
                if e.response.status_code in (429, 502):
                    time.sleep(get_DELAY())
            return None, None
        except Exception as e:
            return None, None
        if response.url == "https://www.linkedin.com/signup":
            return None, None

        soup = BeautifulSoup(response.text, "html.parser")
        div_content = soup.find(
            "div", class_=lambda x: x and "show-more-less-html__markup" in x
        )

        description = None
        if div_content:
            description = " ".join(div_content.get_text().split()).strip()

        def get_job_type(
            soup_job_type: BeautifulSoup,
        ) -> list[JobType] | None:
            """
            Gets the job type from job page
            :param soup_job_type:
            :return: JobType
            """
            h3_tag = soup_job_type.find(
                "h3",
                class_="description__job-criteria-subheader",
                string=lambda text: "Employment type" in text,
            )

            employment_type = None
            if h3_tag:
                employment_type_span = h3_tag.find_next_sibling(
                    "span",
                    class_="description__job-criteria-text description__job-criteria-text--criteria",
                )
                if employment_type_span:
                    employment_type = employment_type_span.get_text(strip=True)
                    employment_type = employment_type.lower()
                    employment_type = employment_type.replace("-", "")

            return [get_enum_from_job_type(employment_type)] if employment_type else []

        return description, get_job_type(soup)



def process_job(job_card, job_url):
        salary_tag = job_card.find('span', class_='job-search-card__salary-info')

        compensation = None
        # if salary_tag:
        #     salary_text = salary_tag.get_text(separator=' ').strip()
        #     salary_values = [currency_parser(value) for value in salary_text.split('-')]
        #     salary_min = salary_values[0]
        #     salary_max = salary_values[1]
        #     currency = salary_text[0] if salary_text[0] != '$' else 'USD'

        #     compensation = Compensation(
        #         min_amount=int(salary_min),
        #         max_amount=int(salary_max),
        #         currency=currency,
        #     )

        title_tag = job_card.find("span", class_="sr-only")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company_url = (
            urlunparse(urlparse(company_a_tag.get("href"))._replace(query=""))
            if company_a_tag and company_a_tag.has_attr("href")
            else ""
        )
        company = company_a_tag.get_text(strip=True) if company_a_tag else "N/A"

        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        # location = self.get_location(metadata_card)

        datetime_tag = (
            metadata_card.find("time", class_="job-search-card__listdate")
            if metadata_card
            else None
        )
        date_posted = None
        if datetime_tag and "datetime" in datetime_tag.attrs:
            datetime_str = datetime_tag["datetime"]
            try:
                date_posted = datetime.strptime(datetime_str, "%Y-%m-%d")
            except Exception as e:
                date_posted = None
        benefits_tag = job_card.find("span", class_="result-benefits__text")
        benefits = " ".join(benefits_tag.get_text().split()) if benefits_tag else None

        description, job_type = get_job_description(job_url)
        # if not description:
        #     return None

        return {
            "title": title,
            "company_name":company,
            "company_url":company_url,
            "date_posted":date_posted,
            "job_url":job_url,
            "job_type":job_type,
            "benefits":benefits,
            "emails":extract_emails_from_text(description) if description else None,
            "description": description
        }

exit_loop = False
while len(job_list) < results_wanted:
    # print(len(job_list))
    params = {
        "keywords": search_term,
        "location": location,
        "distance": distance,
        "f_WT": 2 if is_remote else None,
        "f_JT": job_type_code(job_type) if job_type else None,
        "pageNum": 0,
        "start": page + offset,
        "f_AL": "true" if easy_apply else None,
    }

    params = {k: v for k, v in params.items() if v is not None}
    # print(params)
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(
                f"{url}/jobs-guest/jobs/api/seeMoreJobPostings/search?",
                params=params,
                allow_redirects=True,
                proxies=proxy,
                timeout=10,
            )
            print(response.request.url)
            if response.status_code in (429, 502):
                time.sleep(get_DELAY())
                retries += 1
                continue
            response.raise_for_status()
            # print(1, response.request.url)
            break
        except requests.HTTPError as e:
            if hasattr(e, "response") and e.response is not None:
                if e.response.status_code in (429, 502):
                    time.sleep(get_DELAY())
                    retries += 1
                    continue
                else:
                    exit_loop = True
                    print(2)
                    break
                    # raise ValueError(
                    #     f"bad response status code: {e.response.status_code}"
                    # )
            else:
                raise
        except ProxyError as e:
            raise ValueError("bad proxy")
        except Exception as e:
            raise ValueError(str(e))
    else:
        # Raise an exception if the maximum number of retries is reached
        raise ValueError(
            "Max retries reached, failed to get a valid response"
        )
    soup = BeautifulSoup(response.text, "html.parser")


    for job_card in soup.find_all("div", class_="base-card"):
        job_url = None
        href_tag = job_card.find("a", class_="base-card__full-link")
        if href_tag and "href" in href_tag.attrs:
            href = href_tag.attrs["href"].split("?")[0]
            job_id = href.split("-")[-1]
            job_url = f"{url}/jobs/view/{job_id}"
        with url_lock:
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
        

        # Call process_job directly without threading
        try:
            job_post = process_job(job_card, job_url)
            if job_post and job_post.get("emails", []) and len(job_post.get("emails", [])):
                print(emails, set(job_post.get("emails", [])))
                print(set(emails).issuperset(set(job_post.get("emails", []))))
                if not set(emails).issuperset(set(job_post.get("emails", []))):
                    job_list.append(job_post)
                    emails.extend(job_post.get("emails", []))
        except Exception as e:
            raise ValueError("Exception occurred while processing jobs")
        

    page += 25
    if exit_loop:
        break

pd.DataFrame(job_list).to_csv('data.csv')
