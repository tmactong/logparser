#!/bin/env python2.7

from thrift import Thrift
from storm.ttypes import (Bolt, SpoutSpec, ShellComponent, NullStruct,
                          JavaObject, ComponentObject, Grouping, GlobalStreamId,
                          ComponentCommon, StreamInfo, StormTopology)

def main():
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

if __name__ == "__main__":
    main()
