#!/bin/env python2.7

from __future__ import print_function

import json
from storm import Nimbus
from thrift import Thrift
from storm.ttypes import (Bolt, SpoutSpec, ShellComponent, NullStruct,
                          JavaObject, ComponentObject, Grouping, GlobalStreamId,
                          ComponentCommon, StreamInfo, StormTopology)
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSSLSocket
from thrift.transport import TTransport
from thrift.transport import TSocket

host = 'r61a16266.cm10'
port = 6627
options = {
    'pystorm.log.path': '/tmp/pystorm/logs',
    'pystorm.log.file': 'storm.log',
    'pystorm.log.level': 'debug'
}

def get_topology():
    #Bolt
    logParserBolt = Bolt(
        bolt_object = ComponentObject(
            shell=ShellComponent(
                execution_command='python2.7',
                script = 'logparser_bolt.py'
            )
        ),
        common = ComponentCommon(
            inputs = {
                GlobalStreamId(
                    componentId='kafka_spout',
                    streamId='default'
                ): Grouping(local_or_shuffle=NullStruct())},
            streams = {},
            parallelism_hint = 10
        )
    )
    #Spout
    KafkaSpout = SpoutSpec(
        spout_object = ComponentObject(
            java_object = JavaObject(
                full_class_name = 'logparser.spout.KafkaSpoutGenerator',
                args_list = []
            )
        ),
        common = ComponentCommon(
            inputs = {},
            streams = {
                'default': StreamInfo(
                    output_fields = ['rawlog'],
                    direct = False
                )
            },
            parallelism_hint = 10
        )
    )
    #topology
    topology = StormTopology(
        spouts = {
            'kafka_spout': KafkaSpout
        },
        bolts = {
            'LogParserBolt': logParserBolt
        },
        state_spouts = {}
    )
    return topology

def submit_topology(topology):
    socket = TSocket.TSocket(host, port)
#    transport = TTransport.TBufferedTransport(socket)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    TBinaryProtocol.TBinaryProtocol(transport)
    client = Nimbus.Client(protocol)
    transport.open()
    client.submitTopology(
        'log-parser',
        '/apsarapangu/disk1/storm/nimbus/inbox/stormjar-7f6ac761-8a86-4fee-8920-6e3c8a0fedff.jar',
        json.dumps(options),
        topology
    )
    transport.close()

if __name__ == "__main__":
    topology = get_topology()
    submit_topology(topology)
