import os
import subprocess

from charms.reactive import when, when_not, set_state
from charms.reactive.helpers import data_changed
from charms.reactive import RelationBase
from charmhelpers.core import host
from charmhelpers.core import hookenv

from jujubigdata import utils

from charms.layer.apache_spark import Spark
from charms.layer.hadoop_client import get_dist_config


@when('spark.installed')
@when_not('insightedge-spark.configured')
def configure_insightedge_spark():
    hookenv.status_set('maintenance', 'configuring insightedge')
    dc = get_dist_config()
    destination = dc.path('spark')
    spark = Spark(dc)
    with host.chdir(destination):
        insightedge_jars = subprocess.check_output([
            'bash', '-c',
            '. {}; get_libs ,'.format(
                destination / 'sbin' / 'common-insightedge.sh'
            )
        ], env={'INSIGHTEDGE_HOME': destination}).decode('utf8')
    spark.register_classpaths(insightedge_jars.split(','))
    set_state('insightedge-spark.configured')


@when('insightedge-spark.configured')
def restart_services():
    dc = get_dist_config()
    spark = Spark(dc)
    peers = RelationBase.from_state('sparkpeers.joined')
    is_scaled = peers and len(peers.get_nodes()) > 0
    is_master = spark.is_master()
    is_slave = not is_master or not is_scaled
    master_url = spark.get_master()
    master_ip = spark.get_master_ip()
    if data_changed('insightedge.master_url', master_url):
        stop_datagrid_services()
        start_datagrid_services(master_url, master_ip,
                                is_master, is_slave)
    set_state('insightedge.ready')
    hookenv.status_set('active', 'ready')


def start_datagrid_services(master_url, master_ip, is_master, is_slave):
    # TODO:
    #   * some of the below settings should be exposed as charm config
    dc = get_dist_config()
    ie_home = dc.path('spark')
    if is_master:
        subprocess.call([ie_home / "sbin" / "start-datagrid-master.sh",
                         "--master", master_ip,
                         "--size", "1G"])
    if is_slave:
        subprocess.call([ie_home / "sbin" / "start-datagrid-slave.sh",
                         "--master", master_url,
                         "--locator", "{}:4174".format(master_ip),
                         "--group", "insightedge",
                         "--name", "insightedge-space",
                         "--topology", "2,0",
                         "--size", "1G",
                         "--instances", "id=1;id=2"])


def stop_datagrid_services():
    dc = get_dist_config()
    ie_home = dc.path('spark')
    if utils.jps("insightedge.marker=master"):
        d = dict(os.environ)
        d["TIMEOUT"] = str(10)
        subprocess.call([ie_home / "sbin" / "stop-datagrid-master.sh"],
                        env=d)
    if utils.jps("insightedge.marker=slave"):
        subprocess.call([ie_home / "sbin" / "stop-datagrid-slave.sh"])
