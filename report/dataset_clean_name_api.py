import requests

url_format = 'https://dx.tgdex.telangana.gov.in/tgdex/cat/v1/item?id={}'
def get_uuid_from_dataset_name(dataset_name):
    return dataset_name.split('.')[0]

def get_dataset_name_from_url(uuid, url_format=url_format):
    # uuid = url.split('=')[-1]
    url= url_format.format(uuid)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(url, headers=headers)
    response_json = response.json()
    results = response_json.get('results', [])
    dataset_uuid = get_uuid_from_dataset_name(uuid)
    if results:
        true_name = results[0].get('label', None)
    else:
        true_name = None
    return true_name, dataset_uuid

