"""


cd company_search
scrapy crawl search



"""

import json, tqdm, re, sys 
import scrapy
import tldextract
import spacy 

from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup


class SearchSpider(scrapy.Spider):
    name = "search"
    allowed_domains = []

    query = '"株式会社"　USBメモリ'
    num_results = 400
    url = f"https://www.google.com/search?q={query}&num={num_results}"
    start_urls = [url]
    out_file_path = "./company_names.tsv"

    # Separation for BeautifulSoup.
    my_sep = "<My_Separation_314159>"
    # NLP parameters.
    target_words = ["株式会社", "合弁会社", "合同会社", "合資会社", ]
    nlp = spacy.load('ja_ginza')

    # Debugging.
    url_debug = None
    #url_debug = "https://www.mamoru-kun.com/tips/is-usb-disposal-safety/"
    #url_debug = "https://www.city.amagasaki.hyogo.jp/kurashi/seikatusien/1027475/1030947.html"

    def __init__(self):
        self.counter = 0
        with open(self.out_file_path, "w") as file:
            file.write("\t".join(["query", "url", "companies"]) + "\n")
        return 

    def write(self, atoms):
        with open(self.out_file_path, "a") as file:
            file.write("\t".join(atoms) + "\n")
        return 
    
    def extract_domain(self, url): # Different from urlparse(url).netloc .
        extracted = tldextract.extract(url)
        return "{}.{}".format(extracted.domain, extracted.suffix)
    
    def get_root_url_by_tld(self, target_url):
        target_domain = extract_domain(target_url)
        return urlparse(target_url).scheme + "://" + target_domain

    def get_root_url_by_netloc(self, target_url):
        target_domain = urlparse(target_url).netloc
        return urlparse(target_url).scheme + "://" + target_domain
        
    def is_valid_url(self, url):
        return self.extract_domain(url) not in ["google.com"]

    def start_requests(self):
        if self.url_debug is not None:
            url = self.url_debug
            yield scrapy.Request(
                    url, callback = self.parse_page, 
                    meta = {}, 
                    )
            sys.exit()
        
        for url in self.start_urls:
            # ここでリクエストにカスタムヘッダーを追加することができます
            yield scrapy.Request(
                    url, callback = self.parse_search, 
                    meta = {}, 
                    )
        return
    
    def parse_search(self, response):

        if response.status == 200:
            # ページ内のリンクを抽出
            link_extractor = LinkExtractor()
            links = link_extractor.extract_links(response)
            
            # Sanitize URLs.
            urls = [link.url for link in links if self.is_valid_url(link.url)]
            urls = list(set(urls))
            self.logger.info("MyURLs: " + json.dumps(urls, indent = 4))

            for url in tqdm.tqdm(urls):
                yield scrapy.Request(
                        url, callback = self.parse_page, 
                        meta = {}, 
                        )
        return 

    def find_target_index(self, sent):
        # Get index of token containing the target word.
        idx_target = -1
        for i in range(len(sent)):
            token = sent[i]
            if token.orth_ in self.target_words:
                idx_target = i
                break
        return idx_target

    def get_company_from_sentence(self, sent, debug = False):
        """ Assume the sentence contain the word "株式会社" .
        """
        if self.url_debug is not None: debug = True

        def print_token(token):
            atoms = [token.i, token.orth_, 
                    token.lemma_, token.pos_, 
                    token.tag_, token.dep_, 
                    token.head.i, ]
            self.logger.info(
                    " ".join([str(atom) for atom in atoms]) + "\n"
                    )
        def print_sent(indices, sent):
            self.logger.info("sent: " + ",".join(["%s[%d]" % (sent[i].orth_, i) for i in indices]))

        company = ""
        # Get index of token containing the target word.
        idx_target = self.find_target_index(sent)
        
        # MyLine divide by \t, \n, and space.
        def is_break_token(token):
            flag = False
            if token.pos_ in ["ADP", "AUX", "VERB", ]: flag = True
            else: 
                if token.orth_ in ["・"]:
                    # This is acceptable.
                    pass
                else:
                    if token.pos_ == "PUNCT": flag = True
                    if token.pos_ == "SYM": flag = True
                    if token.tag_ in ["補助記号-一般"]: flag = True
                    if token.pos_ == "NOUN": 
                        if token.tag_ in ["接尾辞-名詞的-一般"]: flag = True
            return flag
        
        if idx_target >= 0:
            # If the target word was found.
            # Assume XXXX株式会社 and 株式会社XXXX patterns.
            indices_before = list()
            indices_after = list()
            if debug: self.logger.info(f"{idx_target=}")
            if debug: print_sent(range(len(sent)), sent)

            # Find the longest non-break symbols around the target.
            if idx_target > 0:
                indices = range(idx_target - 1, -1, -1)
                if debug: self.logger.info("Backward loop.")
                if debug: print_sent(indices, sent)
                for i in indices:
                    token = sent[i]
                    if debug: print_token(token)
                    if is_break_token(token):
                        self.logger.info("BreakToken:")
                        print_token(token)
                        break
                    else:
                        indices_before.append(i)
            if (idx_target + 1) < len(sent):
                indices = range(idx_target + 1, len(sent), 1)
                if debug: self.logger.info("Forward loop.")
                if debug: print_sent(indices, sent)
                for i in indices:
                    token = sent[i]
                    if debug: print_token(token)
                    if is_break_token(token):
                        self.logger.info("BreakToken:")
                        print_token(token)
                        break
                    else:
                        indices_after.append(i)

            def count_dep(tokens, label = "ROOT"):
                return len(token for token in tokens if token.dep_ == label)
            
            len_b, len_a = len(indices_before), len(indices_after)
            if len_b > 0 or len_a > 0:
                mode = "before"
                if len_b > len_a: mode = "before"
                elif len_b < len_a: mode = "after"
                elif len_b == len_a:
                    # token.dep_ に ROOT が少なく compound が多い。
                    root_b = count_dep([sent[i] for i in indices_before], "ROOT")
                    root_a = count_dep([sent[i] for i in indices_after], "ROOT")
                    comp_b = count_dep([sent[i] for i in indices_before], "compound")
                    comp_a = count_dep([sent[i] for i in indices_after], "compound")
                    if root_b < root_a: mode = "before"
                    elif root_b > root_a: mode = "after"
                    else:
                        if comp_b > comp_a: mode = "before"
                        elif comp_b < comp_a: mode = "after"
                
                indices_final = list()
                if mode == "after":
                    indices_final = [idx_target] + indices_after
                elif mode == "before":
                    indices_final = sorted(indices_before) + [idx_target]
                company = "".join([sent[i].orth_ for i in indices_final])
                if debug: self.logger.info("Company: " + company)
        return company
    
    def contains_target_word(self, line):
        return any(word in line for word in self.target_words)

    def parse_page(self, response):
        
        if response.status == 200:
            self.counter += 1
            soup = BeautifulSoup(response.text, 'html.parser')
            text = ''.join(soup.get_text(self.my_sep))
            # Whether any of the target words (Co. Ltd) is contained in the text.
            if self.contains_target_word(response.text):
                # Define multiple split symbols.
                lines = list(set([line for line in re.split(r'[\t\n\s]|%s' % (self.my_sep), text)]))

                companies = list()
                for line in lines:
                    if self.contains_target_word(line):
                        self.logger.info("MyLine: " + line)
                        document = self.nlp(line)
                        
                        for sent in document.sents:
                            company = self.get_company_from_sentence(sent)
                            if len(company) > 0:
                                companies.append(company)
                                self.logger.info("Company: " + company)
                companies = list(set(companies))
                if len(companies) > 0:
                    self.logger.info("Analysis: " + json.dumps({
                            "url": response.url, 
                            "companies": companies, 
                            }, 
                            indent = 4, ensure_ascii = False))
                    self.write([self.query, response.url, str(companies)])
        return 
    
    def finish_by_custom(self):
        self.logger.info("%d pages successfully crawled."% (self.counter))
        return 
    
    # This is overriding the original method.
    def closed(self, reason):
        # ここに終了時の処理を書く
        self.finish_by_custom()
        self.logger.info('Spider closed due to: %s', reason)
