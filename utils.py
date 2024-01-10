import os
import requests
import re

PROXIES = {}


def get_valid_domain_suffixes():
    local_cache_file = "tlds-alpha-by-domain.txt"
    if os.path.exists(local_cache_file):
        txt = open(local_cache_file, "rt").read()
    else:
        url = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"
        txt = requests.get(url=url).text
        open(local_cache_file, "wt").write(txt)

    domains = [i.strip().upper() for i in txt.splitlines() if not i.startswith("#")]
    return domains


def clean_html1(html):
    import html2text

    html = html.replace("<script>", "<ignore>")
    html = html.replace("</script>", "</ignore>")
    # print(html)
    h = html2text.HTML2Text()
    h.ignore_links = True
    # h.ignore_images = True
    # h.ignore_emphasis = True
    pure_text = h.handle(html)


def clean_html(html):
    from bs4.element import Comment
    from bs4 import BeautifulSoup

    def tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    soup = BeautifulSoup(html, 'html.parser')
    texts = soup.findAll(string=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)


suffixes = get_valid_domain_suffixes()


def fetch_domains(url):

    resp = requests.get(url,proxies=PROXIES)
    
    text = resp.text
    ctype = (resp.headers["content-type"])
    if ctype.find("html") > -1:
        pure_text = clean_html(text)
    else:
        pure_text = text

    pattern = re.compile("(?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.+[A-Za-z]{2,6}")

    all_domains = [domain.lower() for domain in pattern.findall(pure_text)]

    result = set()
    for domain in all_domains:
        suffix = domain.split(".")[-1].upper()
        if suffix in suffixes:
            result.add(domain)

    return list(result)


def is_distinct(lst):
    return len(set(lst)) == len(lst)


def make_chunks(lst, length):
    for i in range(0, len(lst), length):
        yield lst[i:i + length]
