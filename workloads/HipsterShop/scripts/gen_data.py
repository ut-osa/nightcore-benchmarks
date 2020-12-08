import os
import sys
import random
import string
import json

NUM_PRODUCTS = 1000
NUM_CATEGORIES = 50

def to_hex(value):
    return '%04x' % value

def random_str(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

if __name__ == '__main__':
    output_dir = sys.argv[1]
    all_categories = list(range(NUM_CATEGORIES))
    products = []
    pids_by_categories = [[] for _ in range(NUM_CATEGORIES)]
    for i in range(NUM_PRODUCTS):
        pid = to_hex(i+1)
        categories = random.sample(all_categories, random.randint(1, 3))
        product = {
            'id': pid,
            'name': random_str(8),
            'description': random_str(20),
            'picture': '/static/img/products/%s.jpg' % pid,
            'priceUsd': {
                'currencyCode': 'USD',
                'units': random.randint(10, 100),
                'nanos': random.randint(200000000, 1000000000)
            },
            'categories': list(map(lambda x: 'cat_'+to_hex(x), categories))
        }
        for category in categories:
            pids_by_categories[category].append(pid)
        products.append(product)
    ads = {}
    for i in range(NUM_CATEGORIES):
        sub_ads = []
        for pid in pids_by_categories[i]:
            sub_ads.append({
                'redirect_url': '/product/%s' % pid,
                'text': random_str(16)
            })
        ads['cat_'+to_hex(i)] = sub_ads
    with open(os.path.join(output_dir, 'products.json'), 'w') as fout:
        json.dump({'products': products}, fout, indent=4, sort_keys=True)
    with open(os.path.join(output_dir, 'ads.json'), 'w') as fout:
        json.dump(ads, fout, indent=4, sort_keys=True)
