# Extract information from legislation

from rich import print
import rich_click as click
from rich.progress import track
from playwright.sync_api import sync_playwright
import sys
import re
import os.path
from parsel import Selector
from datetime import datetime, date
from tinydb import TinyDB, Query
import json
import requests

from typing import Optional, Dict
import pydantic
import tempfile
import configparser
import pytesseract
from pdf2image import convert_from_path, convert_from_bytes
from functools import partial

re_onclik_url = re.compile(r'(window.open|mUtil.winLeft)\("([^"]+)".*')


HEADER = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,es-MX;q=0.7,en;q=0.3",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
}


def ocr_pdf_file(pdf_file, text_file, njobs=4):
    with open(text_file, "w") as f:
        with tempfile.TemporaryDirectory() as path:
            images = convert_from_path(pdf_file, output_folder=path)
            for text_page in map(
                partial(pytesseract.image_to_string, **{"lang": "spa"}), images
            ):
                f.write(text_page)


def serialize_datetime(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError("Type not serializable")


class Options(pydantic.BaseModel):
    query: Optional[str] = '"Inteligencia Artificial"'
    database_filename: Optional[str] = "iniciativas_ia.tinydb.json"
    legislation: Optional[str] = "LXIV"
    document_type: Optional[str] = "Iniciativa"
    download_dir: Optional[str] = "pdfs"


class Info_link(pydantic.BaseModel):
    text: str = None
    url: str = None


class Result(pydantic.BaseModel):
    num: int = None
    subject_type: str = None
    subject: Info_link = None
    subject_info: Optional[Dict] = None
    classification: str = None
    presented_in: str = None
    presented_date: date = None
    presented_by: Info_link = None
    party: str = None
    legislation: str = None
    turn_to: Info_link = None
    status: Info_link = None
    topic: str = None


DEFAULT_SECTION = "DEFAULT"


@click.group()
@click.option("--config-filename", type=click.Path(), default="config.ini")
@click.option("--config-section", type=str, default=DEFAULT_SECTION)
@click.option("-v", "--verbose", is_flag=True, help="Verbose mode")
@click.pass_context
def legislation_mx(ctx, **args):
    ctx.ensure_object(dict)

    config_filename = args["config_filename"]
    config_section = args["config_section"]

    config = configparser.ConfigParser()
    if os.path.exists(config_filename):
        config.read(config_filename)
        if "verbose" in args:
            print(f"[blue]Reading config file from: [/] {config_filename}")
    else:
        print(
            f"[yellow]Warning '{config_filename}' not found found; using default config[/] [white bold]: {config}[/]"
        )
    if config_section not in config:
        print(f"[red]Error '{config_section}' section not found[/]")
        sys.exit(100)

    config = Options(**config[config_section]).model_dump()
    config.update({k: v for k, v in args.items() if v})
    ctx.obj["options"] = config


@legislation_mx.command()
@click.option("--database_filename", type=str, help="Database filename")
@click.pass_context
def list_records(
    ctx,
    **args,
):
    """Print records"""
    # Build config from arguments & config file
    config = dict(ctx.obj["options"].items())
    config.update({k: v for k, v in args.items() if v})

    # Opens database
    db = TinyDB(config["database_filename"], encoding="utf-8")
    for r in db:
        result = Result(**r)
        print(
            f"""\
            * [blue]{result.subject.text}[/]
              {result.presented_by.text} --- {result.presented_date}
              {"✓" if result.subject_info else "No info for subject"}{"✓" if result.subject_info and len(result.subject_info['links'])>0 else "No info for subject"}
            """
        )
    print(f"[green]Total: {len(db)}[/]")


@legislation_mx.command()
@click.option("--database_filename", type=str, help="Database filename")
@click.option("--download_dir", type=str, help="Directory where to download pdfs")
@click.pass_context
def download_pdfs(
    ctx,
    **args,
):
    """Download_pdfs"""
    # Build config from arguments & config file
    config = dict(ctx.obj["options"].items())
    config.update({k: v for k, v in args.items() if not v is None})

    # Opens database
    db = TinyDB(config["database_filename"], encoding="utf-8")

    os.makedirs(config["download_dir"], exist_ok=True)
    for r in track(db):
        result = Result(**r)
        if result.subject_info and len(result.subject_info["links"]) > 0:
            url = result.subject_info["links"][0]
            r_ = requests.get(url, stream=True)
            pdf_file = os.path.join(
                config["download_dir"],
                f"{result.presented_date}-{result.subject.text[:30]}-{result.presented_by.text}.pdf",
            )
            text_file = os.path.join(
                config["download_dir"],
                f"{result.presented_date}-{result.subject.text[:30]}-{result.presented_by.text}.txt",
            )
            if not os.path.exists(pdf_file):
                with open(pdf_file, "wb") as fd:
                    for chunk in r_.iter_content(2000):
                        fd.write(chunk)
            if not os.path.exists(text_file):
                ocr_pdf_file(pdf_file, text_file)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db.update(
                {
                    "pdf_url": url,
                    "pdf_filename": pdf_file,
                    "text_filename": text_file,
                    "modified": current_time,
                },
                doc_ids=[r.doc_id],
            )


@legislation_mx.command()
@click.option("--database_filename", type=str, help="Database filename")
@click.option(
    "--save_into_db/--dont_save_into_db", default=True, help="Save the info into the db"
)
@click.pass_context
def fill_subject(
    ctx,
    **args,
):
    """Fill information about the subject the database"""
    # Build config from arguments & config file
    config = dict(ctx.obj["options"].items())
    config.update({k: v for k, v in args.items() if not v is None})

    # Opens database
    db = TinyDB(config["database_filename"], encoding="utf-8")
    if config["verbose"]:
        print(f"[blue]Loading DB:[/] {config['database_filename']}")
    updated = 0
    Record = Query()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        for r in db:
            result = Result(**r)
            if config["verbose"]:
                print(f"[blue]Visiting page with results:[/] {result.subject.url}")

            page.goto(result.subject.url)
            html_content = page.content()
            selector = Selector(html_content)

            keys = selector.xpath("//tr/td[starts-with(@class,'tdcriterio')]")
            values = selector.xpath("//tr/td[starts-with(@class,'tddatos')]")
            links = selector.xpath(
                "//tr/td[starts-with(@class,'tddatos')]/a/@href"
            ).getall()

            info = {}
            keys = list(keys)
            values = list(values)
            for key, value in zip(keys[1:], values):
                key = key.css("*::text").extract()
                values = value.css("*::text").extract()
                info["".join(k.strip() for k in key)] = " ".join(
                    [v.strip() for v in values]
                )
            info["links"] = links

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db.update(
                {"subject_info": info, "modified": current_time},
                (Record.subject.text == result.subject.text)
                & (Record.presented_date == str(result.presented_date))
                & (Record.presented_by.text == result.presented_by.text),
            )

            updated += 1

        browser.close()


@legislation_mx.command()
@click.argument("q", type=str)
@click.option("--database_filename", type=str, help="Database filename")
@click.option("--legislation", type=str, help="Legislation")
@click.option("--document_type", type=str, help="Document_type")
@click.option("--purge_db", is_flag=True)
@click.option(
    "--save_into_db/--dont_save_into_db", default=True, help="Save the info into the db"
)
@click.pass_context
def query(
    ctx,
    q,
    **args,
):
    row_keys = [
        "num",
        "subject_type",
        "subject",
        "classification",
        "presented_in",
        "presented_date",
        "presented_by",
        "party",
        "legislation",
        "turn_to",
        "status",
        "topic",
    ]

    """Query the database"""
    # Build config from arguments & config file
    config = dict(ctx.obj["options"].items())
    config.update({k: v for k, v in args.items() if not v is None})

    # Opens database
    db = TinyDB(config["database_filename"], encoding="utf-8")
    if config["verbose"]:
        print(f"[blue]Loading DB:[/] {config['database_filename']}")
    if config["purge_db"]:
        print(f"[yellow]Purging the DB:[/] {config['database_filename']}")
        db.truncate()
        print(f"[yellow]At this moment total documents:[/] {len(db)}")
    Record = Query()
    inserted = 0
    updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        # Search through the Advanced search
        url = "http://sil.gobernacion.gob.mx/Busquedas/Avanzada/avanzada.php"
        page.goto(url)
        if config["verbose"]:
            print(f"[blue]Visiting:[/] {url}")
        if config["verbose"]:
            print(f"[blue]Page title:[/]  {page.title()}")
        if config["verbose"]:
            print(
                f"[blue]Selecting:[/] {config['legislation']} and {config['document_type']}"
            )
        # Select main options and put query
        page.select_option("select#LEGISLATURA", label=f"{config['legislation']}")
        page.select_option("select#tipo", label=f"{config['document_type']}")
        page.fill('input[name="VALOR_TEXTO"]', q)

        # Click on search
        with page.expect_popup() as popup_info:
            page.click('input[type="image"][value="Continuar"]')

        # Obtain first result page
        first_page = popup_info.value
        next_url = first_page.url

        # While next url extract results
        while next_url:
            if config["verbose"]:
                print(f"[blue]Visiting page with results:[/] {next_url}")

            page.goto(next_url)
            html_content = page.content()
            selector = Selector(html_content)

            table_rows = selector.css("table tr")

            for row in table_rows:
                columns = row.xpath("td[starts-with(@class,'tddatos')]")
                info = {}
                if not len(columns) == 12:
                    continue
                for i, col in enumerate(columns):
                    text = col.css("*::text").extract()
                    text = " ".join(text)
                    text = text.replace("\xa0", "").strip()
                    if i == 5:
                        text = datetime.strptime(text, "%d/%m/%Y")
                    elif i in [2, 6, 9, 10]:
                        link = col.css("a::attr(onclick)").get()
                        if link:
                            m = re_onclik_url.match(link)
                            if m:
                                link = "http://sil.gobernacion.gob.mx" + m.groups(0)[1]
                            else:
                                link = ""
                        else:
                            link = ""
                        text = Info_link(**{"text": text, "url": link})
                    info[row_keys[i]] = text
                result = Result(**info)
                records = db.search(
                    (Record.subject.text == result.subject.text)
                    & (Record.presented_date == str(result.presented_date))
                    & (Record.presented_by.text == result.presented_by.text)
                )
                if len(records) > 0:
                    if config["verbose"]:
                        print(
                            "[yellow]Record already present in DB, updating status[/]"
                        )
                        print(f"[yellow]{result.subject.text}[/]")

                    if config["save_into_db"]:
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        db.update(
                            {"status": result.status.dict(), "modified": current_time},
                            (Record.subject.text == result.subject.text)
                            & (Record.presented_date == str(result.presented_date))
                            & (Record.presented_by.text == result.presented_by.text),
                        )
                        updated += 1
                else:
                    data = json.dumps(result.dict(), default=serialize_datetime)
                    data = json.loads(data)
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    data["created"] = current_time
                    data["modified"] = current_time
                    if config["save_into_db"]:
                        db.insert(data)
                        inserted += 1

            next_url = selector.xpath(
                '//a[.//strong[contains(text(), ">")]]/@href'
            ).get()
            if next_url:
                next_url = "http://sil.gobernacion.gob.mx" + next_url

        print(f"[blue]Total records inserted: {inserted}")
        print(f"[blue]Total records updated: {updated}")
        browser.close()


if __name__ == "__main__":
    legislation_mx(obj={})
