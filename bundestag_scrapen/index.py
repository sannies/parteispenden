import json
import os
from datetime import datetime, timezone
from typing import Dict

import boto3
import requests

from bs4 import BeautifulSoup

# URL der Webseite mit der Tabelle
url = 'https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2024/2024-inhalt-984862'

pds_url = 'https://bsky.social'

def bsky_login_session(pds_url: str, handle: str, password: str) -> Dict:
    resp = requests.post(
        pds_url + "/xrpc/com.atproto.server.createSession",
        json={"identifier": handle, "password": password},
    )
    resp.raise_for_status()
    return resp.json()


def get_user_password():
    # Erstelle einen Secrets Manager-Client
    client = boto3.client('secretsmanager', region_name='eu-central-1')
    response = client.get_secret_value(SecretId=os.environ.get('BSKY_LOGIN'))

    # Überprüfen, ob das Geheimnis ein String oder JSON ist
    return json.loads(response['SecretString'])


def handler(event, context):
    print("Parteispenden2024-Handler wurde aufgerufen.")
    rows = hole_spenden()
    print(f"{len(rows)} Spenden gefunden.")
    dynamodb = boto3.resource("dynamodb")
    was_ist_getan = dynamodb.Table(os.environ.get("WAS_IST_GETAN_TABELLE"))
    was_ist_getan.get_item(Key={"id": "parteispenden2024"})
    res = was_ist_getan.get_item(Key={"id": "parteispenden2024"})
    if "Item" in res:
        counter = int(res["Item"]["counter"])
    else:
        counter = 0
    print(f'Die ersten  {counter} Spenden wurden bereits gepostet.')

    noch_senden = rows[:-counter] if counter > 0 else rows

    for row in noch_senden[::-1]:
        auf_bsky_posten(row)
        was_ist_getan.update_item(
            Key={"id": "parteispenden2024"},
            UpdateExpression="ADD #counter :increment",
            ExpressionAttributeNames={"#counter": "counter"},
            ExpressionAttributeValues={":increment": 1},
        )


def auf_bsky_posten(row):
    print('Posting: ' +  'Parteispende von {} an die {} in Höhe von {} am {}'.format(
                    row['Donor'], row['Party'], row['Amount'], row['Date Received'].strftime('%d.%m.%Y')
                ))

    up = get_user_password()
    login_session  = bsky_login_session(pds_url, up['username'], up['password'])
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    post = {
        "$type": "app.bsky.feed.post",
        "text": 'Parteispende von {} an die {} in Höhe von {} am {}'.format(
                    row['Donor'], row['Party'], row['Amount'], row['Date Received'].strftime('%d.%m.%Y')
                ),
        "createdAt": now,
    }

    resp = requests.post(
        pds_url + "/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": "Bearer " + login_session["accessJwt"]},
        json={
            "repo":  login_session["did"],
            "collection": "app.bsky.feed.post",
            "record": post,
        },
    )
    print("createRecord response:")
    print(json.dumps(resp.json(), indent=2))


def hole_spenden():
    # HTTP-Header für die Anfrage
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # HTTP-GET-Anfrage an die Webseite
    response = requests.get(url, headers=headers)

    # Überprüfen, ob die Anfrage erfolgreich war
    if response.status_code == 200:
        # Inhalt der Webseite parsen
        soup = BeautifulSoup(response.content, 'html.parser')

        # Die Tabelle anhand ihres CSS-Selektors finden
        table = soup.select_one('table.table')

        # Überprüfen, ob die Tabelle gefunden wurde
        if table:
            # Tabellenzeilen extrahieren
            rows = table.find_all('tr')

            # Listen für die Spaltenüberschriften und Daten erstellen
            headers = []
            data = []

            # Spaltenüberschriften aus der ersten Zeile extrahieren
            for th in rows[0].find_all('th'):
                headers.append(th.get_text(strip=True))

            # Daten aus den restlichen Zeilen extrahieren
            for row in rows[1:]:
                if len(row) > 1:
                    cells = row.find_all(['td', 'th'])
                    data.append([cell.get_text() for cell in cells])

            # DataFrame mit den extrahierten Daten erstellen

            # Define a function to parse dates
            def parse_date(date_str):
                try:
                    return datetime.strptime(date_str, '%d.%m.%Y')
                except ValueError:
                    return None  # Return None for invalid dates

            # Process data and parse dates
            processed_data = []
            for row in data:
                if len(row) == 5:  # Ensure the row has all expected columns
                    party, amount, donor, date_received, date_published = row
                    processed_row = {
                        'Party': party,
                        'Amount': amount,
                        'Donor': donor,
                        'Date Received': parse_date(date_received),
                        'Date Published': parse_date(date_published),
                    }
                    processed_data.append(processed_row)

            return processed_data
        else:
            raise Exception('Tabelle nicht gefunden.')
    else:
        raise Exception(f'Fehler beim Abrufen der Seite: {response.status_code}')


if __name__ == '__main__':
    handler(None, None)