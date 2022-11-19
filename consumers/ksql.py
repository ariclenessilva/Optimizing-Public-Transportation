"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstiles(
  station_id INT,
  station_name VARCHAR,
  line VARCHAR
)WITH (
  KAFKA_TOPIC='org.chicago.cta.turnstiles',
  VALUE_FORMAT='AVRO',
  KEY='station_id'
);
CREATE TABLE turnstile_summary
WITH(
  VALUE_FORMAT='JSON'
) AS SELECT
  station_id, COUNT(*) as count
FROM turnstiles
GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print("TURNSTILE_SUMMARY topic exist")
        logging.debug("TURNSTILE_SUMMARY topic exist")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
    except:
        logging.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logging.debug("TURNSTILE_SUMMARY successfully.")


if __name__ == "__main__":
    execute_statement()
