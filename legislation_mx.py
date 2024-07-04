# Extract information from legislation

from rich import print
from rich.progress import track
import rich_click as click
from playwright.sync_api import sync_playwright
import sys
import re
import os.path
from parsel import Selector
from datetime import datetime, date
from tinydb import TinyDB, Query
import json

from typing import Optional
import pydantic
import configparser

re_onclik_url = re.compile(r'(window.open|mUtil.winLeft)\("([^"]+)".*')


def serialize_datetime(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError("Type not serializable")


class Options(pydantic.BaseModel):
    query: Optional[str] = '"Inteligencia Artificial"'
    database_filename: Optional[str] = "iniciativas_ia.tinydb.json"
    legislation: Optional[str] = "LXIV"
    document_type: Optional[str] = "Iniciativa"


class Info_link(pydantic.BaseModel):
    text: str = None
    url: str = None


class Result(pydantic.BaseModel):
    num: int = None
    subject_type: str = None
    subject: Info_link = None
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
@click.option("--ini_year", type=int, help="Initial year")
@click.option("--fin_year", type=int, help="Final year")
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
        if (
            int(r["year"]) >= config["ini_year"]
            and int(r["year"]) <= config["fin_year"]
        ):
            print(r)
    print(f"[green]Total: {len(db)}[/]")


@legislation_mx.command()
@click.option("--database_filename", type=str, help="Database filename")
@click.option("--query", type=str, help="Query")
@click.option("--legislation", type=str, help="Legislation")
@click.option("--document_type", type=str, help="Document_type")
@click.option(
    "--save_into_db/--dont_save_into_db", default=True, help="Save the info into the db"
)
@click.pass_context
def query(
    ctx,
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
    config.update({k: v for k, v in args.items() if v})

    # Opens database
    db = TinyDB(config["database_filename"], encoding="utf-8")
    if config["verbose"]:
        print(f"[blue]Loading DB:[/] {config['database_filename']}")
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
        page.fill('input[name="VALOR_TEXTO"]', config["query"])

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
                        m = re_onclik_url.match(link)
                        if m:
                            link = "http://sil.gobernacion.gob.mx" + m.groups(0)[1]
                        else:
                            link = ""
                        text = Info_link(**{"text": text, "url": link})
                    info[row_keys[i]] = text
                result = Result(**info)
                records = db.search(Record.subject.text == result.subject.text)
                if len(records) > 0:
                    if config["verbose"]:
                        print(
                            "[yellow]Record already present in DB, updating status[/]"
                        )
                        print(f"[yellow]{result.subject.text}[/]")

                    db.update(
                        {"status": result.status.dict()},
                        Record.subject.text == result.subject.text,
                    )
                    updated += 1
                else:
                    data = json.dumps(result.dict(), default=serialize_datetime)
                    data = json.loads(data)
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
