import paho.mqtt.client as mqtt
import justpy as jp
import asyncio
import psycopg2
from datetime import datetime
import time
import os

app = jp.app

def on_connect(client, userdata, flags, rc, _):
    #print("Connected with result code "+str(rc))
    #print(client,userdata,flags,rc,_)
    client.subscribe("vsaw-wind/messwerte/luv/+")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    topic = msg.topic.split("/")[-1]
    timestamp, payload = payload.split(",",1)
    #t = datetime.fromtimestamp(int(timestamp))
    t = int(timestamp) * 1000
    # switch depending on true, apparent, gps
    if topic == "wahrer-wind":
        twd, tws = map(float, payload.split(","))

        for i in range(len(chart_true_wind_speed_15.options.series[0].data)):
            if chart_true_wind_speed_15.options.series[0].data[0][0] < t - 15 * 60 * 1000:
                chart_true_wind_speed_15.options.series[0].data.pop(0)
            else:
                break
        chart_true_wind_speed_15.options.series[0].data.append([t, tws])

        for i in range(len(chart_true_wind_direction_15.options.series[0].data)):
            if chart_true_wind_direction_15.options.series[0].data[0][0] < t - 15 * 60 * 1000:
                chart_true_wind_direction_15.options.series[0].data.pop(0)
            else:
                break
        chart_true_wind_direction_15.options.series[0].data.append([t, twd])

        true_wind_speed_field.text = "{:.1f}".format(tws)
        true_wind_direction_field.text = "{:.0f}".format(twd)

        # TODO: calculate base wind, outliers

        query = 'INSERT INTO "TrueWind" (timestamp, direction, speed) VALUES (%s, %s, %s);'

        cur.execute(query, (int(timestamp), twd, tws))
        conn.commit()

    elif topic == "scheinbarer-wind":
        awd, aws = map(float, payload.split(","))

        for i in range(len(chart_apparent_wind_speed_15.options.series[0].data)):
            if chart_apparent_wind_speed_15.options.series[0].data[0][0] < t - 15 * 60 * 1000:
                chart_apparent_wind_speed_15.options.series[0].data.pop(0)
            else:
                break
        chart_apparent_wind_speed_15.options.series[0].data.append([t, aws])

        for i in range(len(chart_apparent_wind_direction_15.options.series[0].data)):
            if chart_apparent_wind_direction_15.options.series[0].data[0][0] < t - 15 * 60 * 1000:
                chart_apparent_wind_direction_15.options.series[0].data.pop(0)
            else:
                break
        chart_apparent_wind_direction_15.options.series[0].data.append([t, awd])

        apparent_wind_speed_field.text = "{:.1f}".format(aws)
        apparent_wind_direction_field.text = "{:.0f}".format(awd)

        query = 'INSERT INTO "ApparentWind" (timestamp, direction, speed) VALUES (%s, %s, %s);'

        cur.execute(query, (int(timestamp), awd, aws))
        conn.commit()

    elif topic == "position":
        #lat, lat_dir, lon, lon_dir, spd_over_grnd, heading = map(float, payload.split(","))
        pass
    elif topic == "wetter":
        pass

    # store message in database
    print(payload, t)

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
        "time": {
            "useUTC": False,
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
        "time": {
            "useUTC": False,
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
    client = mqtt.Client(client_id="", userdata=None, protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message
    client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
    client.username_pw_set("server", "in7EYv5##nIr")
    client.connect("9954c15a40844950974ce0e3a9ec731f.s1.eu.hivemq.cloud", 8883)
    client.loop_start()
    print("MQTT Broker connected")

def init_db():
    queryTrueWind = """
        CREATE TABLE IF NOT EXISTS "TrueWind" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "direction" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL
        )
    """

    queryApparentWind = """
        CREATE TABLE IF NOT EXISTS "ApparentWind" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "direction" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL
        )
    """

    queryPosition = """
        CREATE TABLE IF NOT EXISTS "Position" (
          "id" SERIAL primary key,
          "timestamp" INT NOT NULL,
          "lat" DOUBLE PRECISION NOT NULL,
          "lon" DOUBLE PRECISION NOT NULL,
          "heading" DOUBLE PRECISION NOT NULL,
          "speed" DOUBLE PRECISION NOT NULL
        )
    """

    cur.execute(queryTrueWind)
    cur.execute(queryApparentWind)
    cur.execute(queryPosition)
    con.commit()

def build_box(ancestor, label, basis="50%", text_classes="text-7xl"):
    container = jp.Div(a=ancestor, classes="relative flex w-full grow content-center justify-center p-10", style=f"flex-basis: {basis}")
    jp.Label(text=label, a=container, classes="text-md absolute top-2 w-full text-center")
    return jp.Div(text="NaN", a=container, classes=text_classes)

DATABASE_URL = os.environ.get("DATABASE_URL")
con = psycopg2.connect(DATABASE_URL)
cur = con.cursor()
init_db()

wp = jp.WebPage(delete_flag=False)
container = jp.Div(a=wp, classes="flex flex-wrap divide-x divide-y border-b font-mono")
true_wind_direction_field              = build_box(ancestor=container, label="TWD (°):")
true_wind_speed_field                  = build_box(ancestor=container, label="TWS (kt):")
true_wind_direction_5min_avg_field     = build_box(ancestor=container, label="TWD 5 Min Avg (°):")
true_wind_speed_5min_avg_field         = build_box(ancestor=container, label="TWS 5 Min Avg (kt):")
apparent_wind_direction_field          = build_box(ancestor=container, label="AWD (°):")
apparent_wind_speed_field              = build_box(ancestor=container, label="AWS (kt):")
apparent_wind_direction_5min_avg_field = build_box(ancestor=container, label="AWD 5 Min Avg (°):")
apparent_wind_speed_5min_avg_field     = build_box(ancestor=container, label="AWS 5 Min Avg (kt):")
gps_position_container                 = build_box(ancestor=container, label="Position (LAT/LON):", basis="33.333%", text_classes="text-right text-5xl")
gps_position_container.text = ""
gps_lat_container           = jp.Div(a=gps_position_container)
gps_lat_field               = jp.Span(a=gps_lat_container)
gps_lat_dir_field           = jp.Span(a=gps_lat_container)
gps_lon_container           = jp.Div(a=gps_position_container)
gps_lon_field               = jp.Span(a=gps_lon_container)
gps_lon_dir_field           = jp.Span(a=gps_lon_container)
gps_speed_field                        = build_box(ancestor=container, label="SOG (kt):", basis="33.333%")
gps_heading_field                      = build_box(ancestor=container, label="HDG (°):", basis="33.333%")

chart_true_wind_speed_15 = jp.HighCharts(a=wp, options=get_wind_spd_chart_dict("Wahre Windgeschwindigkeit letzte 15 Minuten"), classes='p-2 border w-full')
chart_true_wind_speed_15.update_animation = False
chart_true_wind_direction_15 = jp.HighCharts(a=wp, options=get_wind_dir_dict("Wahre Windrichtung letzte 15 Minuten"), classes='p-2 border w-full')
chart_true_wind_direction_15.update_animation = False

chart_apparent_wind_speed_15 = jp.HighCharts(a=wp, options=get_wind_spd_chart_dict("Scheinbare Windgeschwindigkeit letzte 15 Minuten"), classes='p-2 border w-full')
chart_apparent_wind_speed_15.update_animation = False
chart_apparent_wind_direction_15 = jp.HighCharts(a=wp, options=get_wind_dir_dict("Scheinbare Windrichtung letzte 15 Minuten"), classes='p-2 border w-full')
chart_apparent_wind_direction_15.update_animation = False

def combine_forces(angle1, force1, angle2, force2):
	vector1 = (force1 * math.cos(math.radians(angle1)), force1 * math.sin(math.radians(angle1)))
	vector2 = (force2 * math.cos(math.radians(angle2)), force2 * math.sin(math.radians(angle2)))
	vector_r = (vector1[0] + vector2[0], vector1[1] + vector2[1])
	result_angle = (math.degrees(math.atan2(vector_r[1], vector_r[0]))) % 360
	result_magnitude = math.sqrt(vector_r[0]**2 + vector_r[1]**2)
	return (round(result_angle, 2), round(result_magnitude, 2))

async def periodic_updater():
    while True:
        jp.run_task(wp.update())
        await asyncio.sleep(5)

async def start_updater():
    jp.run_task(periodic_updater())

@jp.SetRoute('/')
async def chart_test():
    # load true wind from db
    query = 'SELECT * FROM "TrueWind" WHERE timestamp >= ' + str(int(time.time()) - 15*60) + " ORDER BY timestamp ASC;"
    cur.execute(query)
    results = cur.fetchall()

    print(results)

    # chart_true_wind_speed_15.

    # load apparent wind from db

    return wp

#if __name__ == '__main__':
mqtt_connect()
jp.justpy(chart_test, startup=start_updater, host="0.0.0.0", port=8000, start_server=False)


# TODO:
#  - store in db and load from db
#  - calculate 5 min avg
#  - filter out gusts in graph
