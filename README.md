# es_helper
## about
Some elasticsearch scripts for devops. </br>

## environment
Ubuntu 14.04 </br>
python 2.7 </br>
elasticsearch 5.x </br>

## dependence
### ubuntu:

* **python2.7** </br>
  sudo apt-get install python-pip python-dev
* **pip package** </br>
  sudo pip install -r requirements.txt

## tools introduction
    run each app： python xxx.py </br>
* **es\_import** </br>
    function： bulk data to es </br>
    details： string\_data in json\_file should be in type of dict\_list, such as：

        [{"name":"zhangsan", "age":13},{"name":"lisi", "age":14}]

    params need to be modified： <font color="#FF0000">es, index\_name, data\_type, json\_file\_name without suffix</font>
* **es\_export** </br>
    function： bulk es\_data to json\_file </br>
    details： params need to be modified： <font color="#FF0000">es, index\_name, data\_type, doc\_from, doc\_size</font>
* **es\_template** </br>
    function： make template </br>
    details： params need to be modified： <font color="#FF0000">host, index\_name, data\_type, template\_body</font>
* **es\_crontab** </br>
	function： crontab for devops
	details： see each script

