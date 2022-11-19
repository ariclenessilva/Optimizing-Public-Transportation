"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("postgres_conn_stations", value_type=Station)

out_topic = app.topic("stations.table",
                      partitions=1,
                      key_type=str,
                      value_type=TransformedStation)

# Define a Faust Table
table = app.Table(
     "transformed_station",
     default=TransformedStation,
     partitions=1,
     changelog_topic=out_topic,
)

@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        if station.red:
            linecolor = 'red'
        elif station.blue:
            linecolor = 'blue'
        elif station.green:
            linecolor = 'green'
        else:
            linecolor = 'unknown'

        transformed_station = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = linecolor
        )

        await out_topic.send(key=station.station_name, value=transformed_station)



if __name__ == "__main__":
    app.main()
