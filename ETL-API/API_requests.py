import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

def paginated_getter():
    page_number = 1
    while True:
        params = {'page': page_number}
        try:
            response = requests.get(BASE_API_URL, params=params)
            response.raise_for_status()
            page_json = response.json()
            print(f'Got page {page_number} with {len(page_json)} records')

            if page_json:
                yield page_json
                page_number += 1
            else:
                break
        except Exception as e:
            print(e)
            break


for page_data in paginated_getter():
    print(page_data)
    break