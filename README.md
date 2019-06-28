# ES Assistant
## About
Some elasticsearch scripts for devops. </br>

## Environment
Ubuntu 14.04 </br>
Python 2.7 </br>
Elasticsearch 5.x </br>

## Dependence
### Ubuntu:

* **python2.7** </br>
  sudo apt-get install python-pip python-dev
* **pip package** </br>
  sudo pip install -r requirements.txt

## Tools introduction
* ### usage
```bash
run each app： python xxx.py
```
* ### es_import
  function： bulk data to es

* ### es_export
    function： bulk es_data to json_file

* ### es_template
    function： make template

* ### es_crontab
	function： crontab for devops

## History
* 2019.06.28
  * add white_list in delete_index_by_disk
* 2018.12.25
  * add cluster_health_check: split-brain problem alert
* 2018.11.20
  * clean code 
* 2018.09.30 
  * add delete_index_by_disk
  * cluster dick check
* 2018.03.15
  * first commit