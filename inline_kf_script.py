from pathlib import Path
import requests
import re
import logging
import numbers
import pandas as pd
from dataclasses import dataclass

@dataclass(frozen=True)
class KauflandStore:
    id: int
    adresa: str

BASE_URL = 'https://www.kaufland.hr/akcije-novosti/popis-mpc.html'

PRICE_MAP = {
        # old name: new name
        "maloprod.cijena(EUR)": "price",
        "cijena jed.mj.(EUR)": "unit_price",
        "MPC poseb.oblik prod": "special_price",
        "Najniža MPC u 30dana": "best_price_30",
        "Sidrena cijena": "anchor_price_date"
    }

FIELD_MAP = {
        "naziv proizvoda": "product_name",
        "šifra proizvoda": "product_id",
        "marka proizvoda": "brand",
        "akc.cijena, A=akcija": "is_akcija",
        "jed.mj. (1 KOM/L/KG)": "jed_mj",
        "kol.jed.mj.": "kol_jed_mj",
        "neto količina(KG)": "quantity",
        "jedinica mjere": "unit",
        "barkod": "barcode",
        "WG": "category"
    }

def find_assetlist_url_static(base_url):
    """
    Fetches the HTML and searches for the dynamic assetList_*.json URL.
    """

    response = requests.get(base_url)
    response.raise_for_status() # Raise an exception for bad status codes

    html_content = response.text

    match = re.search(r'["\']?assetList_(\d+)\.json["\']?', html_content)

    if match:
        dynamic_number = match.group(1)
        return dynamic_number
    else:
        print("File pattern not found in static HTML.")

def normalize_filename_txt_kf(x: str):
    """
    filenames contain metadata on stores (ids, cities,...) and dates, parse those
    """
    
    # Manual replacements
    replacements = {'Dugo_Selo': 'Dugo Selo',
                    'Slavonski_Brod': 'Slavonski Brod',
                    'Velika_Gorica': 'Velika Gorica',
                    "Nova_Gradiska": "Nova Gradiska",
                    "Zagreb_Blato": "Zagreb Blato"}
    
    x = x.removesuffix(".csv").strip()
    
    for old, new in replacements.items():
            x = x.replace(old, new)
    
    x = re.split(r'_+', x)
    
    return x

def filename_structure_match_kf(parts: list) -> dict:
    """ get metadata from filename """
    match parts:
        case [hiper_or_super, *address_parts, city, some_number, date, time]:
            return {
                "store_size": hiper_or_super,
                "address": " ".join(address_parts),
                "city": city,
                "store_id": some_number,
                "date": date,
                "time": time
            }
        case _:
            raise ValueError(f"Invalid component structure: {parts}")

def fetch_stores_dates():
    url = f"https://www.kaufland.hr/akcije-novosti/popis-mpc.assetSearch.id=assetList_{dynamic_number}.json"

    response = requests.get(url)

    csv_links = response.json()

    filenames = [item["label"] for item in csv_links]

    normalized_filenames =[normalize_filename_txt_kf(name) for name in filenames]

    metadata = pd.DataFrame([filename_structure_match_kf(nf) for nf in normalized_filenames])

    df_url = metadata.assign(
        url=[("https://www.kaufland.hr" + item["path"]) for item in csv_links],
        date=pd.to_datetime(metadata["date"], format="%d%m%Y"),
        store_id=pd.to_numeric(metadata["store_id"]).astype("Int16")
    )
    
    return df_url

def read_csv_kf(filename):
    """
    use tab separator and comma decimal
    enconding: try utf-8, fall back to win-1250
    """
    try:
        # Try UTF-8 first
        df = pd.read_csv(filename, delimiter="\t", decimal=",", encoding="utf-8",
                         dtype={"Najniža MPC u 30dana": str})
        print(f"UTF-8   : {filename}")
        return df
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(filename, delimiter="\t", decimal=",", encoding="windows-1250",
                             dtype={"Najniža MPC u 30dana": str})
            print(f"WIN-1250: {filename}")
            return df
        except Exception as e:
            print(f"Both encodings failed: {e}")
            return None

def replace_with_dot_if_number(x):
    # import numbers
    if pd.isna(x):
        return pd.NA
    if not isinstance(x, numbers.Number):
        if len(x) == 0:
            return pd.NA
        else:
            return x.replace(",", ".")
    else:
        return x

def prepare_anchor(s: str):
    if pd.isna(s):
        logging.warning("Encountered missing value")
        return pd.Series([pd.NA, pd.NA])
    if s.count('=') != 1:
        logging.warning(f"No or multiple '=' found in: {s}")
        return pd.Series([pd.NA, pd.NA])
    date, price = s.split('=', 1)
    return pd.Series([date.strip(), price.strip()])

def tidy(df):
        
        df["best_price_30"] = (
            df["best_price_30"]
            .str.replace(r"[^\d\.\,\-]", "", regex=True) # sanitize numbers, allow comma and dot
            .pipe(pd.to_numeric, errors="coerce")
        )

        df[["date_to_parse", "price_to_parse"]] = df.anchor_price_date.apply(prepare_anchor)

        df["anchor_date"] = pd.to_datetime(
                df["date_to_parse"].str.removeprefix("MPC").str.strip(),
                format="mixed", dayfirst=True, errors="coerce")

        df["anchor_price"] = pd.to_numeric(
                df["price_to_parse"]
                .str.removesuffix("€")
                .str.removesuffix("€ur")
                .map(replace_with_dot_if_number))
        
        # df["best_price_30"] = pd.to_numeric(
        #         df["best_price_30"]
        #         # .str.removeprefix("*")
        #         .map(replace_with_dot_if_number))

        df = df.assign(
                product_name = df["product_name"].str.upper(),
                quantity = pd.to_numeric(df["quantity"]),
                kol_jed_mj = pd.to_numeric(df["kol_jed_mj"], downcast="integer"),
                price_anchor_diff = (df["price"] - df["anchor_price"]) / df["anchor_price"],
                is_akcija = pd.to_numeric(df["is_akcija"].replace("A", "1").fillna("0"), downcast="integer"))
                
        return (df
        .convert_dtypes()
        .drop(columns=["anchor_price_date", "date_to_parse", "price_to_parse"]))

def FILT_FAVORITES(df):
        FILT_FAVORITES = (
                df.product_name.str.contains("PILSNER U") & df.product_name.str.contains("PB|4X") |  # obuhvaća staklenu bocu i 4xlimenke
                df.product_name.str.contains("GARDEN") & df.product_name.str.contains("%") |
                df.product_name.str.contains("ARBORIO|CARNAROLI|ORIGINARIO") & df.brand.str.contains("Riso Scotti") |
                df.product_name.str.contains("TOFU") |
                df.product_name.str.contains("TJESTENINA") & df.brand.str.startswith("K-Fav") |
                df.product_name.str.contains("KFAV.FARFALLE|KFAV.LINGUINE") |
                df.product_name.str.contains("MOLISANA") |
                df.product_name.str.startswith("KVEG") & df.product_name.str.contains("NAPITAK ZOB") |
                df.product_name.str.contains("PROSENA KAŠA") |
                df.product_name.str.contains("KAVA") & df.product_name.str.contains("ZRN") & df.product_name.str.contains("BRAS") |
                df.product_name.str.contains("OCTENA ESENCIJA") |
                df.product_name.str.contains("INDOMIE") & df.product_name.str.contains("POVRĆE") |
                df.product_name.str.contains("HUMUS|HUMMUS") & (df.quantity > .15) |
                df.product_name.str.contains("RICE UP") |
                df.product_name.str.contains("KLC.LEĆA") |
                df.product_name.str.contains("TORTERIE") |
                df.product_name.str.contains("BARATTOLINO") |
                df.product_name.str.contains("MASLAC") & df.product_name.str.contains("DUKAT|BREGOV") & (df.quantity > .2) |
                df.product_name.str.contains("KLC.BIO PAP.VREĆA ZA SMEĆE") |
                df.product_name.str.contains("KLC.DETERDŽENT ZA PRA. POSU.U PRAHU") |
                df.product_name.str.contains("KH-7") |
                df.product_name.str.contains("VEDRINI") |
                df.product_name.str.contains("KFAV.ČOKOLADA TAMNA") |
                df.product_name.str.contains("ECOVER") & df.product_name.str.contains("UNI") |
                df.product_name.str.contains("YOGI ČAJ CLASSIC") |
                df.product_name.str.contains("TORTILL") & df.brand.str.contains("K-") |
                df.product_name.str.contains("TORTILL") & df.brand.str.contains("Fiesta") |
                df.product_name.str.contains("PANETTONE") & (df.quantity >= 0.5) |
                df.product_name.str.contains("ELEPHANT SLANO PECIVO SEZAM") & (df.quantity > .15) |
                df.product_name.str.contains("ELEPHANT KREKERI TWIST KARAMEL") & (df.quantity > .15)
                )
        return FILT_FAVORITES

def FILT_WEIZEN(df):
        FILT_WEIZEN = (
                df.product_name.str.contains(
                        "maisels|Krombacher Pivo Weizen|franziskaner|Benediktiner Pivo pše\\.|Benediktiner pšenično pivo|Erdinger Pivo svj\\.|Erdinger Pivo svjet\\.", case=False
                        ))
        return FILT_WEIZEN

def FILT_SIR(df):
        FILT_SIR = (
                (df.product_name.str.contains("halloumi|parmi|pecorino|padano", case=False)) & (df.quantity >= .2))
        return FILT_SIR

def style_dataframe(df: pd.DataFrame,
                    caption=f"UPDATED: {pd.Timestamp.now().strftime("%d.%m.%Y %H:%M")}",
                    header_color="indigo",  #  header_color="#4CAF50",
                    numeric_format=None, hide_index=True):
    """
    Style a DataFrame for HTML output without modifying the original.
    Features:
        - Thin horizontal lines between rows
        - Header color
        - Numeric formatting
    """

    # Basic table style: header + centered cells + borders

    styled = (df.style
        .set_caption(caption)
        .set_table_styles([
        {"selector": "caption", "props": [("caption-side", "top"),
                                          ('font-family', 'Segoe UI, Arial, sans-serif'),
                                          ("font-size", "10px"), 
                                          ("text-align", "left"),
                                          ("padding", "10px")]},
        {'selector': 'th', 'props': [('background-color', header_color),
                             ('color', 'white'),
                             ('text-align', 'center'),
                             ('border-bottom', "2px solid #666"),
                             ('font-family', 'Segoe UI, Arial, sans-serif'),
                             ('font-size', '15px'),
                             ('font-weight', '600'),  # Semi-bold
                             ('letter-spacing', '0.5px'),  # Slight letter spacing
                             ('text-transform', 'uppercase'),  # All caps
                             ('padding', '14px 10px')]},
        {'selector': 'td', 'props': [('padding', '8px'),
                                     ('text-align', 'center'),
                                     ('border-bottom', "1px solid #ccc"),
                                     ('font-family', 'Segoe UI, Arial, sans-serif'),
                                     ('font-size', '15px'),
                                     ('font-weight', '600'),  # Semi-bold
                                     ('letter-spacing', '0.5px')]},  # Slight letter spacing]},
        # {'selector': 'tr:hover', 'props': [('background', 'yellow')]},
    ]))
    
    # styled = styled.format(na_rep="-")
    
    # Numeric formatting
    if numeric_format:
        styled = styled.format(formatter = numeric_format, na_rep="-")
    else:
        styled = styled.format(na_rep="-")

    # Hide index if requested
    if hide_index:
        styled = styled.hide(axis="index")
    
    return styled

def highlight_rows_by_value(row, target_value=1, target_column="is_akcija", highlight_color="CornSilk"):
    """Function to highlight entire row if 'value' == target_value"""
    if row[target_column] == target_value:
        return [f"background-color: {highlight_color}" for _ in row]
    else:
        return ["" for _ in row]


if __name__ == "__main__":
    TODAY = pd.Timestamp.today().normalize()
    KF_ZD = KauflandStore(2030, "Andrije Hebranga 2")
    dynamic_number = find_assetlist_url_static(BASE_URL)
    df_url = fetch_stores_dates()
    url_filtered = df_url[(df_url["date"] == TODAY) & (df_url["store_id"] == KF_ZD.id)].url.squeeze()
    df_in = read_csv_kf(url_filtered)
    df_in = df_in.rename(columns=PRICE_MAP | FIELD_MAP)
    df_in.columns
    df_in.dtypes
    df = tidy(df_in)
    dff = df[(FILT_WEIZEN(df) | FILT_FAVORITES(df) | FILT_SIR(df))]
    
    dff_favs_razlika = (dff
    .astype({col: 'float64' for col in dff.filter(like='price').columns})
    .filter(items=["product_name", "price", 'unit', 'unit_price', "anchor_price", "price_anchor_diff", "is_akcija"])
    .sort_values("price_anchor_diff")
    )

    styled = style_dataframe(
        dff_favs_razlika,
        header_color="#8E44AD", # deep purple
        numeric_format={
            "price": "€{:.2f}",
            "unit_price": "€{:.2f}",
            "anchor_price": "€{:.2f}",
            "price_anchor_diff": "{:.1%}"})

    styled = styled.apply(
            lambda row: highlight_rows_by_value(row, highlight_color="LightBlue"),
            axis=1)

    styled.to_html(Path("output", "index.html"))
