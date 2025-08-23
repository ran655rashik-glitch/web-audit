# scrapy_audit_checklist_deeplink_v2.py
import scrapy
import csv
import hashlib
from urllib.parse import urlparse, urljoin
from scrapy import signals
import random, string

class AuditChecklistDeepLinkV2Spider(scrapy.Spider):
    name = "audit_checklist_deeplink_v2"

    # Exact checklist columns (including the additional fields you requested)
    CSV_FIELDS = [
        "Website URL",
        "Status",
        "Google Index",
        "Robots.txt",
        "HTML Sitemap",
        "XML Sitemap",
        "Custom 404 Page",
        "http & https check",
        "www. & non www version check",
        "Meta Title",
        "Missing Meta Title",
        "Duplicate Meta Title",
        "Meta Description",
        "Missing Meta Description",
        "Duplicate Meta Description",
        "Heading/Sub-headings",
        "Missing Heading (H1)",
        "Multiple Heading (H1)",
        "Image ALT Tags",
        "URL Delimiter Check",
        "URL Friendliness",
        "Absolute vs. Relative URLs",
        "Check for Breadcrumbs",
        "Top Level Navigation (TLN) Analysis",
        "Footer Analysis",
        "Broken Links",
        "Broken Images",
        "Schema Markup",
        "Use of Structured Data Markup",
        "Duplicate Content",
        "Google PageSpeed Insights (desktop)",
        "Google PageSpeed Insights (mobile)",
        "Mobile-Friendly website"
    ]

    # Extra numeric / 1-0 flags and counts (keeps both human-readable and numeric)
    EXTRA_FIELDS = [
        "Broken Links Count",
        "Imgs Missing Alt Count",
        "Missing Meta Title (1/0)",
        "Missing Meta Description (1/0)",
        "Duplicate Meta Title (1/0)",
        "Duplicate Meta Description (1/0)",
        "Duplicate Content (1/0)",
        "Missing Title (1/0)"   # legacy-friendly numeric flag
    ]

    ALL_FIELDS = CSV_FIELDS + EXTRA_FIELDS

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.3,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "LOG_LEVEL": "INFO"
    }

    def __init__(self, start_url=None, sitemap=None, urls_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = []
        self.sitemap = sitemap
        self.urls_file = urls_file
        if urls_file:
            with open(urls_file, "r", encoding="utf-8") as f:
                self.start_urls = [l.strip() for l in f if l.strip()]
        elif start_url:
            self.start_urls = [start_url]
        elif sitemap:
            self.start_urls = [sitemap]
        else:
            raise ValueError("Provide start_url=, sitemap= or urls_file=")

        parsed = urlparse(self.start_urls[0])
        self.site_root = f"{parsed.scheme}://{parsed.netloc}"
        self.allowed_domains = [parsed.netloc]

        # storage
        self.results = []
        self.title_map = {}
        self.meta_map = {}
        self.bodyhash_map = {}
        self.broken_links_map = {}

        self.site_info = {
            "robots_exists": None,
            "html_sitemap_exists": None,
            "xml_sitemap_exists": None,
            "custom_404": None,
            "http_https": None,
            "www_nonwww": None,
        }

    def start_requests(self):
        # site probes
        yield scrapy.Request(urljoin(self.site_root, "/robots.txt"), callback=self._parse_robots, dont_filter=True)
        yield scrapy.Request(urljoin(self.site_root, "/sitemap.xml"), callback=self._parse_sitemap_probe, dont_filter=True)
        yield scrapy.Request(urljoin(self.site_root, "/sitemap_index.xml"), callback=self._parse_sitemap_probe, dont_filter=True)
        yield scrapy.Request(urljoin(self.site_root, "/sitemap.html"), callback=self._parse_sitemap_probe, dont_filter=True)

        randpath = "/.well-known/" + "".join(random.choices(string.ascii_lowercase + string.digits, k=14))
        yield scrapy.Request(urljoin(self.site_root, randpath), callback=self._parse_404_probe, dont_filter=True, errback=self._errback_probe)

        parsed = urlparse(self.site_root)
        if parsed.scheme == "https":
            http_root = f"http://{parsed.netloc}"
            yield scrapy.Request(http_root, callback=self._parse_http_probe, dont_filter=True, errback=self._errback_probe)

        netloc = parsed.netloc
        if netloc.startswith("www."):
            alt = netloc.replace("www.", "")
        else:
            alt = "www." + netloc
        alt_root = f"{parsed.scheme}://{alt}"
        yield scrapy.Request(alt_root, callback=self._parse_www_probe, dont_filter=True, errback=self._errback_probe)

        for u in self.start_urls:
            yield scrapy.Request(u, callback=self.parse, dont_filter=False)

    # probe handlers
    def _parse_robots(self, response):
        self.site_info["robots_exists"] = (response.status == 200 and len(response.text.strip()) > 0)

    def _parse_sitemap_probe(self, response):
        if response.status == 200 and (response.url.endswith(".xml") or "<urlset" in response.text[:2000].lower()):
            self.site_info["xml_sitemap_exists"] = True
        elif response.status == 200 and "<html" in response.text[:2000].lower():
            self.site_info["html_sitemap_exists"] = True

    def _parse_404_probe(self, response):
        if response.status == 404:
            self.site_info["custom_404"] = True
        else:
            lowered = response.text.lower()[:2000]
            if "404" in lowered or "not found" in lowered or "page not found" in lowered:
                self.site_info["custom_404"] = "present_but_200"
            else:
                self.site_info["custom_404"] = False

    def _parse_http_probe(self, response):
        final = response.url
        self.site_info["http_https"] = final.startswith("https://")

    def _parse_www_probe(self, response):
        self.site_info["www_nonwww"] = (response.status == 200)

    def _errback_probe(self, failure):
        return

    # per-page parse
    def parse(self, response):
        # sitemap xml discovery
        if response.url.endswith('.xml') and '<urlset' in response.text[:2000].lower():
            for loc in response.xpath("//url/loc/text()").getall():
                yield scrapy.Request(loc.strip(), callback=self.parse)
            return

        title = (response.xpath("//title/text()").get() or "").strip()
        meta_desc = (response.xpath("//meta[@name='description']/@content").get() or "").strip()
        h1_nodes = response.xpath("//h1")
        h1_count = len(h1_nodes)
        imgs_missing_alt = len(response.xpath("//img[not(@alt)]"))
        breadcrumbs_found = bool(response.xpath("//*[contains(@class,'breadcrumb') or contains(@id,'breadcrumb') or contains(@class,'breadcrumbs') or @role='navigation' and contains(translate(.,'B','b'),'breadcrumb')]").get())
        footer_found = bool(response.xpath("//footer").get())
        url_path = urlparse(response.url).path
        url_delimiter_ok = "_" not in url_path
        links_all = response.xpath("//a/@href").getall()
        rel_count = sum(1 for l in links_all if l and not l.startswith("http"))
        abs_count = sum(1 for l in links_all if l and l.startswith("http"))
        abs_vs_rel = "relative" if rel_count > abs_count else "absolute"

        body_text = "".join(response.xpath("//body//text()").getall()).strip()
        body_hash = hashlib.sha1(body_text.encode("utf-8")).hexdigest() if body_text else ""

        item = {
            "Website URL": response.url,
            "Status": response.status,
            "Google Index": "",
            "Robots.txt": "Yes" if self.site_info.get("robots_exists") else ("No" if self.site_info.get("robots_exists") is False else ""),
            "HTML Sitemap": "Yes" if self.site_info.get("html_sitemap_exists") else ("No" if self.site_info.get("html_sitemap_exists") is False else ""),
            "XML Sitemap": "Yes" if self.site_info.get("xml_sitemap_exists") else ("No" if self.site_info.get("xml_sitemap_exists") is False else ""),
            "Custom 404 Page": ("Yes" if self.site_info.get("custom_404") else ("No" if self.site_info.get("custom_404") is False else "")),
            "http & https check": ("HTTPS" if response.url.startswith("https://") else "HTTP"),
            "www. & non www version check": ("Alternate root reachable" if self.site_info.get("www_nonwww") else ""),
            "Meta Title": title,
            "Missing Meta Title": ("X" if title == "" else "√"),
            "Duplicate Meta Title": "",  # set later
            "Meta Description": meta_desc,
            "Missing Meta Description": ("X" if meta_desc == "" else "√"),
            "Duplicate Meta Description": "",  # set later
            "Heading/Sub-headings": ("Has H1" if h1_count > 0 else "Missing H1"),
            "Missing Heading (H1)": ("X" if h1_count == 0 else ""),
            "Multiple Heading (H1)": ("X" if h1_count > 1 else ""),
            "Image ALT Tags": f"{imgs_missing_alt} missing alt",
            "URL Delimiter Check": ("√" if url_delimiter_ok else "X"),
            "URL Friendliness": "",
            "Absolute vs. Relative URLs": abs_vs_rel,
            "Check for Breadcrumbs": ("√" if breadcrumbs_found else "X"),
            "Top Level Navigation (TLN) Analysis": "",
            "Footer Analysis": ("√" if footer_found else "X"),
            "Broken Links": "",
            "Broken Images": 0,
            "Schema Markup": ("√" if bool(response.xpath("//script[@type='application/ld+json']").get()) else "X"),
            "Use of Structured Data Markup": ("Yes" if bool(response.xpath("//script[@type='application/ld+json']").get()) else "No"),
            "Duplicate Content": "",  # set later
            "Google PageSpeed Insights (desktop)": "",
            "Google PageSpeed Insights (mobile)": "",
            "Mobile-Friendly website": "",
            # extras
            "Broken Links Count": 0,
            "Imgs Missing Alt Count": imgs_missing_alt,
            "Missing Meta Title (1/0)": (1 if title == "" else 0),
            "Missing Meta Description (1/0)": (1 if meta_desc == "" else 0),
            "Duplicate Meta Title (1/0)": 0,
            "Duplicate Meta Description (1/0)": 0,
            "Duplicate Content (1/0)": 0,
            "Missing Title (1/0)": (1 if title == "" else 0)
        }

        t = title.strip() or "(no title)"
        self.title_map.setdefault(t, []).append(response.url)
        md = meta_desc.strip() or "(no meta)"
        self.meta_map.setdefault(md, []).append(response.url)
        if body_hash:
            self.bodyhash_map.setdefault(body_hash, []).append(response.url)

        self.results.append(item)

        base_netloc = urlparse(self.site_root).netloc
        internal_set = set()
        for href in links_all:
            if not href:
                continue
            href_full = urljoin(response.url, href)
            parsed = urlparse(href_full)
            if parsed.netloc == base_netloc:
                internal_set.add(href_full)

        # schedule link-checks (deep link-check)
        for link in internal_set:
            yield scrapy.Request(link,
                                 callback=self.check_link,
                                 meta={'parent': response.url, 'link_target': link},
                                 dont_filter=True,
                                 errback=self._link_errback)

        # continue discovery crawl
        for link in internal_set:
            yield scrapy.Request(link, callback=self.parse, dont_filter=True)

    def check_link(self, response):
        parent = response.meta.get('parent')
        status = response.status
        if status >= 400:
            self.broken_links_map.setdefault(parent, []).append((response.url, status))
        return

    def _link_errback(self, failure):
        request = failure.request
        parent = request.meta.get('parent')
        link = request.meta.get('link_target') or request.url
        self.broken_links_map.setdefault(parent, []).append((link, "ERR"))
        return

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AuditChecklistDeepLinkV2Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        title_dupes = {t for t, urls in self.title_map.items() if len(urls) > 1 and t != "(no title)"}
        meta_dupes = {m for m, urls in self.meta_map.items() if len(urls) > 1 and m != "(no meta)"}
        body_dupe_hashes = {h for h, urls in self.bodyhash_map.items() if len(urls) > 1}

        total_broken_links_site = 0
        total_pages_missing_title = 0
        total_imgs_missing_alt = 0
        total_duplicate_title_pages = 0
        total_duplicate_meta_pages = 0
        total_duplicate_content_pages = 0

        for r in self.results:
            url = r["Website URL"]
            broken_list = self.broken_links_map.get(url, [])
            r["Broken Links Count"] = len(broken_list)
            r["Broken Links"] = "; ".join(f"{t}({s})" for t, s in broken_list[:10])
            total_broken_links_site += len(broken_list)

            total_imgs_missing_alt += r.get("Imgs Missing Alt Count", 0)

            if r.get("Missing Meta Title (1/0)", 0) == 1:
                total_pages_missing_title += 1

            t = r["Meta Title"].strip() or "(no title)"
            if t in title_dupes:
                r["Duplicate Meta Title"] = "X"
                r["Duplicate Meta Title (1/0)"] = 1
                total_duplicate_title_pages += 1
            else:
                r["Duplicate Meta Title (1/0)"] = 0

            md = r["Meta Description"].strip() or "(no meta)"
            if md in meta_dupes:
                r["Duplicate Meta Description"] = "X"
                r["Duplicate Meta Description (1/0)"] = 1
                total_duplicate_meta_pages += 1
            else:
                r["Duplicate Meta Description (1/0)"] = 0

            found_dup = False
            for h, urls in self.bodyhash_map.items():
                if r["Website URL"] in urls and h in body_dupe_hashes:
                    found_dup = True
                    break
            if found_dup:
                r["Duplicate Content"] = "X"
                r["Duplicate Content (1/0)"] = 1
                total_duplicate_content_pages += 1
            else:
                r["Duplicate Content (1/0)"] = 0

        # write CSV
        outcsv = "audit_checklist.csv"
        with open(outcsv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.ALL_FIELDS)
            writer.writeheader()
            for row in self.results:
                outrow = {k: row.get(k, "") for k in self.ALL_FIELDS}
                writer.writerow(outrow)

        # write summary
        summary_path = "audit_checklist_summary.txt"
        with open(summary_path, "w", encoding="utf-8") as s:
            s.write(f"Site root: {self.site_root}\n")
            s.write(f"Total pages crawled: {len(self.results)}\n")
            s.write(f"Total broken links (site-wide): {sum(r['Broken Links Count'] for r in self.results)}\n")
            s.write(f"Total pages missing meta title: {sum(r.get('Missing Meta Title (1/0)',0) for r in self.results)}\n")
            s.write(f"Total images missing alt attributes (sum): {sum(r.get('Imgs Missing Alt Count',0) for r in self.results)}\n")
            s.write(f"Total pages with duplicate meta title: {sum(r.get('Duplicate Meta Title (1/0)',0) for r in self.results)}\n")
            s.write(f"Total pages with duplicate meta description: {sum(r.get('Duplicate Meta Description (1/0)',0) for r in self.results)}\n")
            s.write(f"Total pages with duplicate content: {sum(r.get('Duplicate Content (1/0)',0) for r in self.results)}\n")
            s.write("\nNotes:\n- Google Index / PageSpeed / Mobile-Friendly fields are blank (require Google APIs).\n- 'Broken Links' column contains up to 10 broken targets per page (url(status)).\n- Deep link-check multiplies requests; reduce CONCURRENT_REQUESTS or increase DOWNLOAD_DELAY if crawling a third-party site.\n")

        self.logger.info("Wrote %d rows to %s and summary to %s", len(self.results), outcsv, summary_path)
