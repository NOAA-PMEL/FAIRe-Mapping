import requests
from bs4 import BeautifulSoup

def extract_insdc_geographic_locations() -> list:

        url = 'https://www.insdc.org/submitting-standards/geo_loc_name-qualifier-vocabulary/'

        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        html_content = response.text

        soup = BeautifulSoup(html_content, 'html.parser')

        # Get all elements that fall under the class name
        elements = soup.find_all(class_='wp-block-list has-large-font-size')

        # extract all elements with 'li' in their tag and get the text
        locations = []
        for element in elements:
            li_elements = element.find_all('li', recursive=True)
            for li in li_elements:
                locations.append(li.get_text(strip=True))

        return locations