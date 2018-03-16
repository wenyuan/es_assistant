# es_helper
## about
Some scripts to assist in development and testing. </br>

## environment
Ubuntu 14.04 </br>
python 2.7 </br>
elasticsearch 5.x </br>

## dependence
### ubuntu:

* python2.7 </br>
  sudo apt-get install python-pip python-dev
* pip package </br>
  sudo pip install -r requirements.txt

## usage
    run each app: python xxx.py </br>
* **es\_import:** </br>
    bulk data to es </br>
    string\_data in json\_file should be in type of dict\_list, such as:

        [{"name":"zhangsan", "age":13},{"name":"lisi", "age":14}]

    params need to be modified: es, index_name, data_type
* **es\_export:** </br>
    bulk es\_data to json\_file </br>
    params need to be modified: es, index_name, data_type, doc_from, doc_size
