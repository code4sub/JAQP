
COMPASSHOME=/home/xiliang/COMPASS
COMPASS=$COMPASSHOME

function recreate_topic(){
    ${COMPASSHOME}/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic $1 1>&2 2>/dev/null && echo "Deleted topic "$1 >/dev/null 2>&1
    ${COMPASSHOME}/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic $1
}

function getsize(){
    ${COMPASSHOME}/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $1 > /tmp/tsize
    cat /tmp/tsize|awk -F":" 'BEGIN{sum=0}{sum=sum+$3}END{print sum}'
}
function listt(){
    ${COMPASSHOME}/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
}

function desc(){
    ${COMPASSHOME}/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic $1
}

function populate(){
    datatopic=$1
    datasetpath=$2
    size=$3
    touch /tmp/dummy_pass
    mkdir -p /tmp/pass
    recreate_topic $datatopic
    # recreate_topic "dummy0"
    # recreate_topic "dummy1"
    # recreate_topic "dummy2"
    fname="/tmp/pass/populate${size}.txt"
    echo "insert: $size" > $fname
    echo "sleep: 1" >> $fname
    echo "exit" >> $fname
    echo "generated cmd file:"

    cat $fname
    cd ${COMPASSHOME}/producer
    ./run.sh -csvpath $datasetpath -querypath /tmp/dummy_pass -cmdpath $fname -datatopic $datatopic -querytopic dummy0 -deltopic dummy1 -ticktopic dummy2
    getsize $datatopic
    cd -
}

function genQonly(){
    fname=$1
    qsize=$2
    echo "sleep: 3" > $fname
    echo "t_query: $qsize" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "query: save" >> $fname
    echo "sleep: 3" >> $fname
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}

function genQonlyPerf(){
    fname=$1
    qsize=$2
    echo "sleep: 3" > $fname
    echo "t_query: $qsize" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "sleep: 5" >> $fname
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}

function genID(){
    fname=$1
    idsize=$2
    offset=$3
    echo "sleep: 3" > $fname
    echo "t_insert: $idsize $offset" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "t_delete: $idsize" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}

function genIDQ10(){
    fname=$1
    isize=$2
    offset=$3
    qsize=$4
    totalsize=$5
    echo "sleep: 3" > $fname
    while [ "$offset" -le "$totalsize" ]
    do
        echo "query: rndelete" >> $fname
        echo "sleep: 2" >> $fname
        echo "t_insert: $isize $offset" >> $fname
        echo "sleep: 10" >> $fname
        echo "tock" >> $fname
        echo "t_query: $qsize 0" >> $fname
        echo "sleep: 5" >> $fname
        echo "tock" >> $fname
        echo "query: save" >> $fname
        offset=$(( offset+isize ))
    done
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}

function genIQ10(){
    fname=$1
    isize=$2
    offset=$3
    qsize=$4
    totalsize=$5
    echo "sleep: 3" > $fname
    while [ "$offset" -le "$totalsize" ]
    do
        echo "t_query: $qsize 0" >> $fname
        echo "sleep: 5" >> $fname
        echo "tock" >> $fname
        echo "query: save" >> $fname
        echo "t_insert: $isize $offset" >> $fname
        echo "sleep: 10" >> $fname
        echo "tock" >> $fname
        offset=$(( offset+isize ))
    done
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}

function genIDQ(){
    fname=$1
    idsize=$2
    offset=$3
    qsize=$4
    echo "sleep: 3" > $fname
    echo "t_insert: $idsize $offset" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "t_delete: $idsize" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "t_query: $qsize" >> $fname
    echo "sleep: 10" >> $fname
    echo "tock" >> $fname
    echo "query: exit" >> $fname
    echo "exit" >> $fname
}
