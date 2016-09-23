import subprocess

from charms.reactive import when, when_not, set_state
from charmhelpers.core import host

from charms.layer.apache_spark import Spark
from charms.layer.hadoop_client import get_dist_config


@when('spark.installed')
@when_not('insightedge-spark.configured')
def configure_insightedge_spark():
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
