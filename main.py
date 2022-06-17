import paho.mqtt.client as mqtt
import justpy as jp
import asyncio
import psycopg2
import datetime
import os

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    client.subscribe("vsaw-wind/messwerte/luv/+")

def on_message(client, userdata, msg):
    # switch depending on true, apparent, gps
    payload = msg.payload.decode()
    topic = msg.topic.split("/")[-1]
    timestamp, payload = payload.split(",",1)
    t = datetime.fromtimestamp(timestamp)
    if topic == "wahrer-wind":
        twd, tws = map(float, payload.split(","))

        if len(chart_true_wind_speed_15.options.series[0].data) == 0:
            chart_true_wind_speed_15.options.plotOptions.spline.pointStart = datetime.fromtimestamp(int(timestamp))

        if len(chart_true_wind_speed_15.options.series[0].data) > 15 * 60:
            chart_true_wind_speed_15.options.series[0].data.pop(0)
        chart_true_wind_speed_15.options.series[0].data.append([t, round(tws / 1.852, 2)])

        if len(chart_true_wind_direction_15.options.series[0].data) == 0:
            chart_true_wind_direction_15.options.plotOptions.spline.pointStart = datetime.fromtimestamp(int(timestamp))

        if len(chart_true_wind_direction_15.options.series[0].data) > 15 * 60:
            chart_true_wind_direction_15.options.series[0].data.pop(0)
        chart_true_wind_direction_15.options.series[0].data.append([t, twd])

        # TODO: calculate base wind, outliers

        query = "INSERT INTO 'TrueWind' VALUES(%s, %s, %s);"

        cur.execute(query, (timestamp, twd, tws))
        conn.commit()

    elif topic == "scheinbarer-wind":
        awd, aws = map(float, payload.split(","))

        if len(chart_apparent_wind_speed_15.options.series[0].data) == 0:
            chart_apparent_wind_speed_15.options.plotOptions.spline.pointStart = datetime.fromtimestamp(int(timestamp))

        if len(chart_apparent_wind_speed_15.options.series[0].data) > 15 * 60:
            chart_apparent_wind_speed_15.options.series[0].data.pop(0)
        chart_apparent_wind_speed_15.options.series[0].data.append([t, round(aws / 1.852, 2)])

        if len(chart_apparent_wind_direction_15.options.series[0].data) == 0:
            chart_apparent_wind_direction_15.options.plotOptions.spline.pointStart = datetime.fromtimestamp(int(timestamp))

        if len(chart_apparent_wind_direction_15.options.series[0].data) > 15 * 60:
            chart_apparent_wind_direction_15.options.series[0].data.pop(0)
        chart_apparent_wind_direction_15.options.series[0].data.append([t, awd])

        query = "INSERT INTO 'ApparentWind' VALUES(%s, %s, %s);"

        cur.execute(query, (timestamp, awd, aws))
        conn.commit()

    elif topic == "position":
        lat, lat_dir, lon, lon_dir, spd_over_grnd, heading = map(float, payload.split(","))
        pass
    elif topic == "wetter":
        pass

    # store message in database

def get_wind_spd_chart_dict(title):
    return {
        "chart": {
            "type": 'spline',
            "scrollablePlotArea": {
                "minWidth": 600,
                "scrollPositionX": 1
            }
        },
        "title": {
            "text": title,
            "align": 'left'
        },
        "xAxis": {
            "type": 'datetime',
            "labels": {
                "overflow": 'justify'
            }
        },
        "yAxis": {
            "title": {
                "text": 'Wind Geschwindigkeit (kn)'
            },
            "minorGridLineWidth": 0,
            "gridLineWidth": 0,
            "floor": 0,
            "plotBands": [{ # 1 Bft
                "from": 1,
                "to": 4,
                "color": 'rgba(68, 170, 213, 0.1)',
                "label": {
                    "text": '1 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 2 Bft
                "from": 4,
                "to": 7,
                "color": 'rgba(0, 0, 0, 0)',
                "label": {
                    "text": '2 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 3 Bft
                "from": 7,
                "to": 11,
                "color": 'rgba(68, 170, 213, 0.1)',
                "label": {
                    "text": '3 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 4 Bft
                "from": 11,
                "to": 16,
                "color": 'rgba(0, 0, 0, 0)',
                "label": {
                    "text": '4 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 5 Bft
                "from": 16,
                "to": 22,
                "color": 'rgba(68, 170, 213, 0.1)',
                "label": {
                    "text": '5 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 6 Bft
                "from": 22,
                "to": 28,
                "color": 'rgba(0, 0, 0, 0)',
                "label": {
                    "text": '6 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }, { # 7 Bft
                "from": 28,
                "to": 34,
                "color": 'rgba(68, 170, 213, 0.1)',
                "label": {
                    "text": '7 Bft',
                    "style": {
                        "color": '#606060'
                    }
                }
            }]
        },
        "tooltip": {
            "valueSuffix": ' kn'
        },
        "plotOptions": {
            "spline": {
                "lineWidth": 4,
                "states": {
                    "hover": {
                        "lineWidth": 5
                    }
                },
                "marker": {
                    "enabled": False
                },
                "pointInterval": 1000, # one second
                # "pointStart":
            }
        },
        "series": [{
            "name": 'Basis',
            "data": []

        }, {
            "name": 'Böen',
            "data": []
        }],
        "navigation": {
            "menuItemStyle": {
                "fontSize": '10px'
            }
        }
    }

def get_wind_dir_dict(title):
    return {
        "chart": {
            "type": 'line',
            "scrollablePlotArea": {
                "minWidth": 600,
                "scrollPositionX": 1
            }
        },
        "title": {
            "text": title,
            "align": 'left'
        },
        "xAxis": {
            "type": 'datetime',
            "labels": {
                "overflow": 'justify'
            }
        },
        "yAxis": {
            "title": {
                "text": 'Wind Geschwindigkeit (kn)'
            },
            "minorGridLineWidth": 0,
            "gridLineWidth": 0,
            "ceiling": 360,
            "floor": 0,
        },
        "series": [{ "name": "Richtung", "data": [] }],
    }

def mqtt_connect():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("broker.hivemq.com", 1883)
    client.loop_start()

def init_db():
    queryTrueWind = """
        CREATE TABLE IF NOT EXISTS "TrueWind" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "direction" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL,
        )
    """

    queryApparentWind = """
        CREATE TABLE IF NOT EXISTS "ApparentWind" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "direction" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL,
        )
    """

    queryPosition = """
        CREATE TABLE IF NOT EXISTS "Position" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "lat" DOUBLE PRECISION NOT NULL,
          "lon" DOUBLE PRECISION NOT NULL,
          "heading" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL,
        )
    """

    cur.execute(queryTrueWind)
    cur.execute(queryApparentWind)
    cur.execute(queryPosition)
    con.commit()

DATABASE_URL = os.environ.get("DATABASE_URL")
con = psycopg2.connect(DATABASE_URL)
cur = con.cursor()
init_db()

wp = jp.WebPage(delete_flag=False)

chart_true_wind_speed_15 = jp.HighCharts(a=wp, options=get_wind_spd_chart_dict("Wahre Windgeschwindigkeit letzte 15 Minuten"), classes='m-1 p-2 border w-10/12')
chart_apparent_wind_speed_15 = jp.HighCharts(a=wp, options=get_wind_spd_chart_dict("Scheinbare Windgeschwindigkeit letzte 15 Minuten"), classes='m-1 p-2 border w-10/12')

chart_true_wind_direction_15 = jp.HighCharts(a=wp, options=get_wind_dir_dict("Wahre Windrichtung letzte 15 Minuten"), classes='m-1 p-2 border w-10/12')
chart_apparent_wind_direction_15 = jp.HighCharts(a=wp, options=get_wind_dir_dict("Scheinbare Windrichtung letzte 15 Minuten"), classes='m-1 p-2 border w-10/12')

async def periodic_updater():
    while True:
        jp.run_task(wp.update())
        await asyncio.sleep(5)

async def start_updater():
    jp.run_task(periodic_updater())

async def chart_test():
    # load true wind from db

    # load apparent wind from db

    return wp

if __name__ == '__main__':
    mqtt_connect()
    jp.justpy(chart_test, startup=start_updater, host="0.0.0.0", port=8000)
