import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import time
from tqdm import tqdm
import ssl
from urllib3.exceptions import InsecureRequestWarning

def modify_env_url(url, env):
    """
    Модифицирует URL если, если он не на PROD
    Добавляет в ссылку среду (env)
    """
    if url and url.startswith('https://market.'):
        modified_url = url.replace('https://market.', f'https://market.{env}.', 1)
        return modified_url
    elif url and url.startswith('https://my.'):
        modified_url = url.replace('https://my.', f'https://my.{env}.', 1)
        return modified_url
    elif url and url.startswith('https://edu.'):
        modified_url = url.replace('https://edu.', f'https://edu.{env}.', 1)
        return modified_url
    elif url and url.startswith('https://forum.'):
        modified_url = url.replace('https://forum.', f'https://forum.{env}.', 1)
        return modified_url
    else:
        modified_url = url.replace('https://', f'https://{env}.', 1)
        return modified_url
        
    return url

def check_single_link(row, col1_name, col2_name, col3_name, env):
    page_url = row[col1_name]
    wrong_url = row[col2_name]
    final_url = row[col3_name]

    modified_page_url = modify_env_url(page_url, env)
    modified_wrong_url = modify_env_url(wrong_url, env)
    modified_final_url = modify_env_url(final_url, env)

    result = {
        col1_name: modified_page_url,
        col2_name: modified_wrong_url,
        col3_name: modified_final_url,
        'ссылка из Final Address найдена': False,
        'некорректная ссылка найдена': False,
        'код_ответа': None,
        'ошибка': None
    }

    try:
        response = requests.get(
            modified_page_url,
            timeout=10,
            headers={'User-Agent': 'Mozilla/5.0'},
            allow_redirects=True, 
            verify=False
        )

        result['код_ответа'] = response.status_code

        if response.status_code == 200:
            found_final_url = modified_final_url in response.text
            result['ссылка из Final Address найдена'] = found_final_url
            found_wrong_url = modified_wrong_url in response.text
            result['некорректная ссылка найдена'] = found_wrong_url
        else:
            result['ошибка'] = f'HTTP {response.status_code}'

    except requests.exceptions.RequestException as e:
        result['ошибка'] = str(e)
    
    return result

def check_links_parallel(excel_file, col1_name, col2_name, col3_name, env, output_file='results.xlsx', max_workers=2):
    """
    Параллельная проверка ссылок
    
    Parameters:
    excel_file: путь к Excel файлу
    col1_name: название колонки с URL страниц
    col2_name: название колонки с URL для поиска
    env: среда (test, dev, stage и т.д.)
    output_file: имя файла для сохранения результатов
    max_workers: количество параллельных потоков
    """
    
    df = pd.read_excel(excel_file)
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, row in df.iterrows():
            future = executor.submit(check_single_link, row, col1_name, col2_name, col3_name, env)
            futures.append(future)
        
        with tqdm(total=len(futures), desc="Проверка ссылок") as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)
    
    results_df = pd.DataFrame(results)
    
    column_order = [col1_name, col2_name, col3_name, 'ссылка из Final Address найдена', 'некорректная ссылка найдена', 'код_ответа', 'ошибка']
    results_df = results_df[column_order]
    
    results_df.to_excel(output_file, index=False)
    print(f"\nРезультаты сохранены в {output_file}")
        
    return results_df

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

results = check_links_parallel(
    excel_file='test_links.xlsx',
    col1_name='Source',
    col2_name='Address',
    col3_name='Final Address',
    env='',
    max_workers=12
)