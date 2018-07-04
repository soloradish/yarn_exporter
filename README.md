# Yarn Exporter

## Installation

	git clone git@github.com:soloradish/yarn_exporter.git
	cd yarn_exporter
	pip install -r requirements.txt
	
## Usage

If YARN restful API on http://127.0.0.1:8088

	 >> ./yarn_exporter.py http://127.0.0.1:8088
	 
## Options and Defaults

 - `-H` or `--host`  exporter's listen address, default is `"0.0.0.0"`.
 - `-p` or `--port` exporter's listen port, default is `9459`
 - `-n` or `--cluster-name` yarn cluster's name, default is `"cluster_0""`
 - `-c` or `--collected-apps` yarn applications name, default is `None`

## Count running apps by name

	 >> ./yarn_exporter.py http://127.0.0.1:8088 -c streaming_app1 streaming_app2
	 
Will get additional metrics of running apps count.

	yarn_apps_running_by_name{app_name="streaming_app1",cluster="cluster_0"} 3.0
	
