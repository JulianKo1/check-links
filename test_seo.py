import pytest
from pages.index_page import IndexPage
import pandas as pd
from tqdm import tqdm

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

@pytest.mark.seo_analysis
@pytest.mark.parametrize("initialize_browser_state", ["default"], indirect=True)
def test_seo(index_page_with_state: IndexPage):
    df = pd.read_excel('links.xlsx')
    urls = df['Source'].tolist()
    correct_links = df['Final Address'].tolist()
    wrong_links = df['Address'].tolist()
    results = []
    
    for i, url in enumerate(tqdm(urls)):
        try:
            modified_url = modify_env_url(url, 'test')
            modified_correct_link = modify_env_url(correct_links[i], 'test')
            modified_wrong_link = modify_env_url(wrong_links[i], 'test')
            # responce = index_page_with_state.visit(modified_url)
            with index_page_with_state.page.expect_response(lambda response: response.url == modified_url) as response_info:
                index_page_with_state.visit(modified_url)
            response = response_info.value
            response_status = response.status
            page_content = index_page_with_state.page.content()
            wrong_link_found = 'ERROR' if modified_wrong_link in page_content else 'PASSED'
            correct_link_found = 'PASSED' if modified_correct_link in page_content else 'ERROR'
            
            results.append({
                'url': modified_url,
                'status code': response_status,
                'correct_link': modified_correct_link,
                'wrong_link': modified_wrong_link,
                'wrong link not found': wrong_link_found,
                'correct link found': correct_link_found,
            })
            
        except Exception as e:
            results.append({
                'url': url,
                'text_found': False,
                'error': str(e)
            })
    
    # Сохраняем результаты
    results_df = pd.DataFrame(results)
    results_df.to_excel('seo_check_results.xlsx', index=False)