import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging
import os

if not os.path.exists('./logs'):
    os.makedirs('logs')

logging.basicConfig(
    filename="logs/data_cleaning.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def log_step(step_name):
    logging.info(f"Started: {step_name}")


def log_error(step_name, error):
    logging.error(f"Error in {step_name}: {error}")


def log_success(step_name):
    logging.info(f"Success: {step_name}")


columns_to_keep = [
    "nom",
    "prenom",
    "date_naissance",
    "cin",
    "tel",
    "email",
    "diplome",
    "etablissment",
    "formation",
    "lettre_motivation",
    "etat",
    "viewed",
    "contacte",
    "inscrit",
    "created",
    "ville",
]
log_step("Loading CSV file")
try:
    data = pd.read_csv("data.csv")
    log_success("Loading CSV file")
except Exception as e:
    log_error("Loading CSV file", e)
data = data[columns_to_keep]


def is_valid_name(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^[A-Za-zÀ-ÿ -]*$")
    return bool(pattern.match(value))


def correct_name(value):
    if is_valid_name(value):
        return value.strip().title()
    return None


def is_valid_date(value):
    try:
        pd.to_datetime(value, format="%Y-%m-%d", errors="raise")
        return True
    except (ValueError, TypeError):
        return False


def correct_date(value):
    if is_valid_date(value):
        return value
    else:
        return None


def is_valid_cin(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^[A-Za-z]{1,2}\d{3,}$")
    return bool(pattern.match(value))


def correct_cin(value):
    if isinstance(value, str):
        value = value.replace(" ", "")
    if is_valid_cin(value):
        return value.upper()
    else:
        return None


def is_valid_tel(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^\+2126\d{8}$")
    return bool(pattern.match(value))


def correct_tel(value):
    if is_valid_tel(value):
        return value
    if not pd.isna(value):
        if not isinstance(value, str):
            value = str(int(value))
        pattern = re.compile(r"^6\d{8}$")
        if bool(pattern.match(value)) == True:
            return "+212" + value
        pattern = re.compile(r"^2126\d{8}$")
        if bool(pattern.match(value)) == True:
            return "+" + value
    return None


def is_valid_email(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^[a-zA-Z0-9._-]+@[a-zA-Z]+\.[a-zA-Z]+$")
    return bool(pattern.match(value))


email_domains = [
    "@gmail.com",
    "@yahoo.com",
    "@outlook.com",
    "@hotmail.com",
    "@icloud.com",
    "@aol.com",
    "@protonmail.com",
    "@zoho.com",
    "@gmx.com",
    "@mail.com",
    "@yandex.com",
]


def correct_email(value):
    if is_valid_email(value):
        return value.replace(" ", "").lower()
    elif pd.isna(value) or not isinstance(value, str):
        return None
    else:
        value = value.replace(" ", "").lower()
        if value:
            if "@" in value:
                username, partial_domain = value.split("@")
                partial_domain = "@" + partial_domain
                for domain in email_domains:
                    if domain.startswith(partial_domain):
                        return username + domain
            else:
                return value + "@gmail.com"
    return None


def is_valid_diplome(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^bac\+\d$")
    return bool(pattern.match(value)) or value == "autre"


def correct_diplome(value):
    if not isinstance(value, str) or pd.isna(value):
        return None
    value = value.replace(" ", "").lower()
    if is_valid_diplome(value):
        return value
    else:
        return None


def is_valid_created(value):
    if not isinstance(value, str):
        return False
    try:
        pd.to_datetime(value, format="%Y-%m-%d %H:%M:%S", errors="raise")
        return True
    except ValueError:
        return False


def correct_created(value):
    if is_valid_created(value):
        return value
    else:
        return None


def is_single_digit(value):
    if not isinstance(value, str):
        return False
    pattern = re.compile(r"^\d$")
    return bool(pattern.match(value))


def correct_single_digit(value):
    if is_single_digit(value):
        return value
    else:
        return 0


cin_city = {
    "A": "Rabat",
    "AA": "Rabat",
    "AC": "Rabat",
    "AJ": "Rabat",
    "AB": "Salé",
    "AE": "Salé",
    "AY": "Salé",
    "AS": "Salé",
    "AD": "Témara",
    "B": "Casablanca",
    "BA": "Casablanca",
    "BB": "Casablanca",
    "BE": "Casablanca",
    "BH": "Casablanca",
    "BJ": "Casablanca",
    "BK": "Casablanca",
    "BL": "Casablanca",
    "BM": "Casablanca",
    "BF": "Casablanca",
    "BV": "Casablanca",
    "BW": "Casablanca",
    "C": "Fez",
    "CC": "Fez",
    "CD": "Fez",
    "CB": "Sefrou",
    "CN": "Boulemane",
    "D": "Meknes",
    "DN": "Meknes",
    "DA": "Azrou",
    "DB": "Ifrane",
    "DC": "Moulay Idriss Zerhoun",
    "DJ": "Ain Taoujdate",
    "DN": "El Hajeb",
    "DO": "Ouislane",
    "E": "Marrakesh",
    "EE": "Marrakesh",
    "EA": "Ben Guerir",
    "F": "Oujda",
    "FA": "Berkane",
    "FB": "Taourirt",
    "FC": "El Aioun Sidi Mellouk",
    "FD": "Ain Bni Mathar",
    "FE": "Saïdia",
    "FG": "Figuig",
    "FH": "Jerada",
    "FJ": "Ahfir",
    "FK": "Touissit",
    "FL": "Bouarfa",
    "G": "Kenitra, Sidi Yahya El Gharb",
    "GA": "Sidi Slimane, Sidi Yahya El Gharb",
    "GB": "Souk El Arbaa",
    "GK": "Sidi Kacem",
    "GM": "Ouazzane",
    "GN": "Mechra Bel Ksiri",
    "GJ": "Jorf El Melha",
    "H": "Safi",
    "HH": "Safi",
    "HA": "Youssoufia",
    "I": "Beni Mellal",
    "IA": "Kasba Tadla",
    "IB": "Fquih Ben Saleh",
    "IC": "Azilal",
    "ID": "Souk Sebt Ould Nemma",
    "IE": "Demnate",
    "J": "Agadir",
    "JK": "Agadir",
    "JA": "Guelmim",
    "JB": "Inezgane, Dcheira El Jihadia",
    "JC": "Taroudant",
    "JD": "Sidi Ifni",
    "JE": "Tiznit",
    "JF": "Tan-Tan",
    "JH": "Chtouka Aït Baha",
    "JM": "Aït Melloul, Temsia, Lqliâa, Oulad Dahou",
    "JT": "Oulad Teima",
    "JY": "Tata",
    "JZ": "Assa-Zag",
    "K": "Tangier",
    "KB": "Tangier",
    "KA": "Asilah",
    "L": "Tétouan",
    "LA": "Larache",
    "LB": "Ksar el-Kebir",
    "LC": "Chefchaouen",
    "LE": "Martil",
    "LF": "Fnideq",
    "LG": "M'diq",
    "M": "El Jadida",
    "MA": "Azemmour",
    "MC": "Sidi Bennour",
    "MD": "Zemamra",
    "N": "Essaouira",
    "O": "Dakhla",
    "OD": "Dakhla",
    "P": "Ouarzazate",
    "PA": "Tinghir",
    "PB": "Zagora",
    "Q": "Khouribga",
    "QA": "Oued Zem",
    "R": "Al Hoceima",
    "RB": "Imzouren",
    "RC": "Targuist",
    "RX": "Bni Bouayach",
    "S": "Nador",
    "SA": "Nador",
    "SH": "Laayoune",
    "SJ": "Smara",
    "SK": "Tarfaya",
    "SL": "Boujdour",
    "T": "Mohammedia",
    "TA": "Benslimane",
    "TK": "Benslimane",
    "U": "Errachida",
    "UA": "Goulmima",
    "UB": "Er-Rich",
    "UC": "Erfoud",
    "UD": "Rissani",
    "V": "Khenifra",
    "VA": "Midelt, Itzer",
    "VM": "M'rirt",
    "W": "Settat",
    "WA": "Berrechid",
    "WB": "Ben Ahmed",
    "X": "Khemisset",
    "XA": "Tifelt",
    "Y": "Kalaat Sraghna",
    "Z": "Taza",
    "ZG": "Guercif",
    "ZH": "Karia Ba Mohamed",
    "ZT": "Taounate",
}


def get_ville(cin):
    if is_valid_cin(cin):
        cin_code = None
        pattern = re.compile(r"^[A-Za-z]+")
        match = pattern.match(cin)
        if match:
            cin_code = match.group(0).upper()
            if cin_code in cin_city:
                return cin_city[cin_code]
            else:
                return None
    return None


check_func = {
    "nom": is_valid_name,
    "prenom": is_valid_name,
    "date_naissance": is_valid_date,
    "cin": is_valid_cin,
    "tel": is_valid_tel,
    "email": is_valid_email,
    "diplome": is_valid_diplome,
    "created": is_valid_created,
    "etat": is_single_digit,
    "viewed": is_single_digit,
    "contacte": is_single_digit,
    "inscrit": is_single_digit,
}

correct_func = {
    "nom": correct_name,
    "prenom": correct_name,
    "date_naissance": correct_date,
    "cin": correct_cin,
    "tel": correct_tel,
    "email": correct_email,
    "diplome": correct_diplome,
    "created": correct_created,
    "etat": correct_single_digit,
    "viewed": correct_single_digit,
    "contacte": correct_single_digit,
    "inscrit": correct_single_digit,
}
# log_step("Detecting and correcting anomalies in columns")
# for column in data.columns:
#     if column in check_func:
#         anomalies = data[column].apply(check_func[column])
#         if anomalies.any():
#             logging.info(
#                 f"Anomalies detected in column: {column}. Applying correction function."
#             )
#             data[column] = data[column].apply(correct_func[column])
#             log_success(f"Corrected anomalies in column: {column}")
#         else:
#             logging.info(f"No anomalies detected in column: {column}")
#     else:
#         logging.info(f"No correction function for column: {column}")


log_step("Detecting and correcting anomalies in columns")
for column in data.columns:
    if column in check_func:
        anomalies = data[column].apply(check_func[column])
        if anomalies.any():
            for i, value in enumerate(data[column]):
                if not check_func[column](value):
                    logging.warning(
                        f"Anomaly detected in column {column} at row {i}: Value '{value}' is incorrect."
                    )
                    logging.info(
                        f"Anomalies detected in column {column}. Applying correction function."
                    )
                    old_value = value
                    new_value = correct_func[column](value)
                    data.at[i, column] = new_value
                    if old_value != new_value:
                        logging.info(
                            f"Column {column}, row {i}: Corrected value: {old_value} -> {new_value}"
                        )

            log_success(f"Anomalies corrected in column {column}")
        else:
            logging.info(f"No anomalies detected in column: {column}")

log_step("Interpolating date values in created column")
try:
    data["created"] = pd.to_datetime(data["created"], errors="coerce")
    data["created"] = data["created"].interpolate(method="linear")
    data["created"] = data["created"].dt.round("1s")
    log_success("Interpolating date values in created column")
except Exception as e:
    log_error("Interpolating date values in created column", e)

log_step("Filling column 'ville' based on 'cin' column")
try:
    data["ville"] = data["cin"].apply(get_ville)
    log_success("Filling column 'ville' based on 'cin' column")
except Exception as e:
    log_error("Filling column 'ville' based on 'cin' column", e)

log_step("Removing rows with null values")
try:
    data.dropna(inplace=True, ignore_index=True)
    log_success("Removing rows with null values")
except Exception as e:
    log_error("Removing rows with null values", e)

log_step("Saving cleaned data to CSV")
try:
    data.to_csv("cleaned_file.csv", index=False)
    logging.info("Data saved to 'cleaned_file.csv' successfully")
except Exception as e:
    log_error("Saving cleaned data", str(e))
