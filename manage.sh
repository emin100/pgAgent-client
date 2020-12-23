#!/usr/bin/env bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH
if [[ "$1" == '-i' ]]
then
    if [[ ! -d "logs" ]]
    then
        mkdir logs
    fi

    sudo apt install virtualenv

    virtualenv --python=python2 venv
    source venv/bin/activate
    pip install -r requirements.txt
    cp config.cfg.example config.cfg
    vi config.cfg


    cat <<EOF | sudo tee -a /etc/cron.d/agent
* * * * * root cd `pwd` && flock -n agent.lock ./venv/bin/python agent_run.py
EOF
    sudo service cron restart
elif [[ "$1" == '-k' ]]
then
    kill $(pgrep -a python | grep -i "agent_run.py" | cut -d" " -f1  )
elif [[ "$1" == '-h' ]] || [[ "$1" == '' ]]
then
    echo "-i install"
    echo "-k kill process"
fi
