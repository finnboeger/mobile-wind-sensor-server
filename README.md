# Wind monitoring server

An optional companion for https://github.com/finnboeger/mobile-wind-sensor-client. 
This server can be connected to the same MQTT broker and then show the collected data.

It is currently configured for use with herokus free instance. However since that service has since shut down it will likely change soon.
It can be run using `gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app` as long as the `DATABASE_URL` environment variable has been set and can be used to connect to a postgres database instance.

![server_data](https://user-images.githubusercontent.com/10922421/208658843-eb5bf7ec-b25c-4aa3-9184-ba188dc09ef5.png)
![server_graph_true](https://user-images.githubusercontent.com/10922421/208658857-a8fc082c-e415-4757-8780-0398255db2d7.png)
![server_graph_apparent](https://user-images.githubusercontent.com/10922421/208658848-0664381a-4c6a-4fa3-8730-0955ca38937a.png)
