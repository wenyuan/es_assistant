#!/bin/bash

# ref: http://blog.csdn.net/felix_yujing/article/details/78207667
###################################
#删除早于十天的ES集群的索引
#背景
#需要定期清理的索引的后缀日期格式为YYYY.MM.DD，如：project-index-2017.10.01
#思路
#通过_cat/indices接口可以获取当前ES全部索引信息，取第三列为索引名。过滤出索引名中带有的日期字符串，然后进行日期比较，早于10天前的日期便可通过日期模糊匹配索引来删除。
#用法
#chmod +x delete_index_by_day.sh
#crontab: 0   6 * * *  /{your_dir}/delete_index_by_day.sh >/dev/null 2>&1    # 每天6点执行一次脚本里的命令
###################################

#es配置
es_cluster_ip="127.0.0.1"
keep_days=10
#日志名称
log="/{your_dir}/logs/delete_index_by_day.log"    #操作日志存放路径
fsize=2000000    #如果日志大小超过上限，则保存旧日志，重新生成日志文件
#exec 2>>$log     #如果执行过程中有错误信息均输出到日志文件中

#写日志函数
#参数
    #参数 内容
#返回值 无
w_log(){
    #判断文件是否存在
    if [ ! -e "$log" ]
    then
        touch $log
    fi
    #当前时间
    local curtime;
    curtime=`date +"%Y-%m-%d %H:%M:%S"`
    echo "$curtime $*" >> $log
    #判断文件大小
    local cursize ;
    cursize=`cat $log | wc -c`
    if [ $fsize -lt $cursize ]
    then
        mv $log $log"-"$curtime
        touch $log ;
    fi
}

#删除索引函数
#参数
    #参数一 es主机ip
    #参数二 要删除的索引名
#返回值 无
delete_indices(){
    comp_date=`date -d "$keep_days day ago" +"%Y-%m-%d"`
    date1="$2 00:00:00"
    date2="$comp_date 00:00:00"

    t1=`date -d "$date1" +%s`
    t2=`date -d "$date2" +%s`

    if [ $t1 -le $t2 ]; then
        w_log "索引名以$2结尾的数据早于$comp_date,是$keep_days天以前的数据,将进行索引删除"
        #转换一下格式，将类似2017-10-01格式转化为2017.10.01
        format_date=`echo $2| sed 's/-/\./g'`
        curl -XDELETE http://$1:9200/*$format_date
    fi
}

current_date=$(date +"%Y-%m-%d")
w_log "--------------开始检测----------------"
curl -XGET http://${es_cluster_ip}:9200/_cat/indices | awk -F" " '{print $3}' | awk -F"-" '{print $NF}' | egrep "[0-9]*\.[0-9]*\.[0-9]*" | sort | uniq  | sed 's/\./-/g' | while read LINE
do
    delete_indices ${es_cluster_ip} $LINE
done
w_log "--------------检测完成----------------"
echo " " >> $log
