#!/usr/bin/env python3
from datetime import datetime, time, timedelta
from wsgiref.simple_server import make_server
from collections import defaultdict
import urllib.parse
import argparse

from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from prometheus_client import make_wsgi_app
import attr
import requests

from version import __version__


def mega_to_byte(megabyte: int) -> int:
    # convert megabytes to bytes decimal base
    return int(int(megabyte) * 1_048_576)


def datetime_to_epoch_ms(_datetime: datetime) -> int:
    return int(_datetime.timestamp() * 1000)


@attr.s
class YarnMetric(object):
    GAUGE = 'gauge'
    COUNTER = 'counter'
    supported_type = [GAUGE, COUNTER]

    namespace = "yarn"

    name = attr.ib()
    metric_type = attr.ib()

    @metric_type.validator
    def check(self, _, value):
        if value not in self.supported_type:
            raise ValueError(f'Parameter metric_type value must in {self.supported_type}, can not be {value}')

    description = attr.ib()
    labels = attr.ib(default=attr.Factory(list))

    @property
    def metric_name(self):
        return f'{self.namespace}_{self.name}'

    def create_metric(self):
        if self.metric_type == self.GAUGE:
            return GaugeMetricFamily(self.metric_name, self.description, labels=self.labels)
        elif self.metric_type == self.COUNTER:
            return CounterMetricFamily(self.metric_name, self.description, labels=self.labels)
        else:
            raise ValueError(f'property metric_type value must in {self.supported_type}, can not be {self.metric_type}')


class YarnCollector(object):
    api_path = '/'

    def __init__(self, endpoint, cluster_name='yarn'):
        self.endpoint = endpoint
        self.cluster_name = cluster_name

    @property
    def metric_url(self):
        return urllib.parse.urljoin(self.endpoint, self.api_path)

    def collect(self):
        raise NotImplemented


class YarnClusterInfoCollector(YarnCollector):
    api_path = '/ws/v1/cluster/info'

    def collect(self):
        response = requests.get(self.metric_url)
        response.raise_for_status()
        info = response.json()['clusterInfo']
        cluster_id = str(info['id'])
        cluster_info = YarnMetric('cluster_info', YarnMetric.GAUGE, 'Yarn cluster info',
                                  [
                                      'cluster_name', 'cluster_id', 'state', 'ha_state',
                                      'resource_manager_version', 'resource_manager_build_version',
                                      'hadoop_version', 'hadoop_build_version'
                                  ]).create_metric()
        cluster_info.add_metric([
            self.cluster_name, cluster_id, info['state'], info['haState'], info['resourceManagerVersion'],
            info['resourceManagerBuildVersion'], info['hadoopVersion'], info['hadoopBuildVersion']
        ], 1)
        yield cluster_info


class YarnMetricCollector(YarnCollector):
    api_path = '/ws/v1/cluster/metrics'

    def collect(self):
        response = requests.get(self.metric_url, allow_redirects=True)
        response.raise_for_status()
        metric = response.json()['clusterMetrics']

        apps_submitted = YarnMetric('apps_submitted_total', YarnMetric.COUNTER,
                                    'The number of applications submitted', ['cluster']).create_metric()
        apps_submitted.add_metric([self.cluster_name], metric['appsSubmitted'])
        yield apps_submitted

        apps_completed = YarnMetric('apps_completed_total', YarnMetric.COUNTER,
                                    'The number of applications completed', ['cluster']).create_metric()
        apps_completed.add_metric([self.cluster_name], metric['appsCompleted'])
        yield apps_completed

        apps_failed = YarnMetric('apps_failed_total', YarnMetric.COUNTER,
                                 'The number of applications failed', ['cluster']).create_metric()
        apps_failed.add_metric([self.cluster_name], metric['appsFailed'])
        yield apps_failed

        apps_killed = YarnMetric('apps_killed_total', YarnMetric.COUNTER,
                                 'The number of applications killed', ['cluster']).create_metric()
        apps_killed.add_metric([self.cluster_name], metric['appsKilled'])
        yield apps_killed

        apps_pending = YarnMetric('apps_pending', YarnMetric.GAUGE,
                                  'The number of applications pending', ['cluster']).create_metric()
        apps_pending.add_metric([self.cluster_name], metric['appsPending'])
        yield apps_pending

        apps_running = YarnMetric('apps_running', YarnMetric.GAUGE,
                                  'The number of applications running', ['cluster']).create_metric()
        apps_running.add_metric([self.cluster_name], metric['appsRunning'])
        yield apps_running

        memory_all = YarnMetric('memory_all_bytes', YarnMetric.GAUGE,
                                'The amount of total memory', ['cluster']).create_metric()
        memory_all.add_metric([self.cluster_name], mega_to_byte(metric['totalMB']))
        yield memory_all

        memory_reserved = YarnMetric('memory_reserved_bytes', YarnMetric.GAUGE,
                                     'The amount of memory reserved', ['cluster']).create_metric()
        memory_reserved.add_metric([self.cluster_name], mega_to_byte(metric['reservedMB']))
        yield memory_reserved

        memory_available = YarnMetric('memory_available_bytes', YarnMetric.GAUGE,
                                      'The amount of memory available', ['cluster']).create_metric()
        memory_available.add_metric([self.cluster_name], mega_to_byte(metric['availableMB']))
        yield memory_available

        memory_allocated = YarnMetric('memory_allocated_bytes', YarnMetric.GAUGE,
                                      'The amount of memory allocated', ['cluster']).create_metric()
        memory_allocated.add_metric([self.cluster_name], mega_to_byte(metric['allocatedMB']))
        yield memory_allocated

        cpu_cores_all = YarnMetric('cpu_cores_all', YarnMetric.GAUGE,
                                   'The total number of virtual cores', ['cluster']).create_metric()
        cpu_cores_all.add_metric([self.cluster_name], metric['totalVirtualCores'])
        yield cpu_cores_all

        cpu_cores_reserved = YarnMetric('cpu_cores_reserved', YarnMetric.GAUGE,
                                        'The number of reserved virtual cores', ['cluster']).create_metric()
        cpu_cores_reserved.add_metric([self.cluster_name], metric['reservedVirtualCores'])
        yield cpu_cores_reserved

        cpu_cores_available = YarnMetric('cpu_cores_available', YarnMetric.GAUGE,
                                         'The number of available virtual cores', ['cluster']).create_metric()
        cpu_cores_available.add_metric([self.cluster_name], metric['availableVirtualCores'])
        yield cpu_cores_available

        cpu_cores_allocated = YarnMetric('cpu_cores_allocated', YarnMetric.GAUGE,
                                         'The number of allocated virtual cores', ['cluster']).create_metric()
        cpu_cores_allocated.add_metric([self.cluster_name], metric['allocatedVirtualCores'])
        yield cpu_cores_allocated

        containers_allocated = YarnMetric('containers_allocated', YarnMetric.GAUGE,
                                          'The number of containers allocated', ['cluster']).create_metric()
        containers_allocated.add_metric([self.cluster_name], metric['containersAllocated'])
        yield containers_allocated

        containers_reserved = YarnMetric('containers_reserved', YarnMetric.GAUGE,
                                         'The number of containers reserved', ['cluster']).create_metric()
        containers_reserved.add_metric([self.cluster_name], metric['containersReserved'])
        yield containers_reserved

        containers_pending = YarnMetric('containers_pending', YarnMetric.GAUGE,
                                        'The number of containers pending', ['cluster']).create_metric()
        containers_pending.add_metric([self.cluster_name], metric['containersPending'])
        yield containers_pending

        nodes_all = YarnMetric('nodes_all', YarnMetric.GAUGE,
                               'The total number of nodes', ['cluster']).create_metric()
        nodes_all.add_metric([self.cluster_name], metric['totalNodes'])
        yield nodes_all

        nodes_active = YarnMetric('nodes_active', YarnMetric.GAUGE,
                                  'The number of active nodes', ['cluster']).create_metric()
        nodes_active.add_metric([self.cluster_name], metric['activeNodes'])
        yield nodes_active

        nodes_lost = YarnMetric('nodes_lost', YarnMetric.GAUGE,
                                'The number of lost nodes', ['cluster']).create_metric()
        nodes_lost.add_metric([self.cluster_name], metric['lostNodes'])
        yield nodes_lost

        nodes_unhealthy = YarnMetric('nodes_unhealthy', YarnMetric.GAUGE,
                                     'The number of unhealthy nodes', ['cluster']).create_metric()
        nodes_unhealthy.add_metric([self.cluster_name], metric['unhealthyNodes'])
        yield nodes_unhealthy

        nodes_decommissioned = YarnMetric('nodes_decommissioned', YarnMetric.COUNTER,
                                          'The number of nodes decommissioned', ['cluster']).create_metric()
        nodes_decommissioned.add_metric([self.cluster_name], metric['decommissionedNodes'])
        yield nodes_decommissioned

        nodes_rebooted = YarnMetric('nodes_rebooted', YarnMetric.COUNTER,
                                    'The number of nodes rebooted', ['cluster']).create_metric()
        nodes_rebooted.add_metric([self.cluster_name], metric['rebootedNodes'])
        yield nodes_rebooted


class YarnApplicationCollector(YarnCollector):
    api_path = '/ws/v1/cluster/apps'
    time_buffer = timedelta(minutes=5)

    @property
    def search_time_range(self):
        today = datetime.utcnow().date()
        today_beginning = datetime.combine(today, time.min)
        today_ending = datetime.combine(today, time.max)
        return datetime_to_epoch_ms(today_beginning - self.time_buffer), datetime_to_epoch_ms(
            today_ending - self.time_buffer)

    def collect(self):
        start, end = self.search_time_range
        payload = {'startedTimeBegin': start, 'startedTimeEnd': end}
        response = requests.get(self.metric_url, params=payload)
        response.raise_for_status()


class YarnRunningApplicationCollector(YarnCollector):
    api_path = '/ws/v1/cluster/apps'

    def __init__(self, endpoint, cluster_name, *app_name_filter):
        self.app_names = app_name_filter
        super().__init__(endpoint, cluster_name)

    def collect(self):
        payload = {'states': 'running'}
        response = requests.get(self.metric_url, params=payload)
        response.raise_for_status()

        grouped_apps = defaultdict(int)
        running_apps = YarnMetric('apps_running_by_name', YarnMetric.GAUGE,
                                  'The number of running apps, group by app name',
                                  ['cluster', 'app_name']).create_metric()
        for app_info in response.json()['apps']['app']:
            if app_info['name'] in self.app_names:
                grouped_apps[app_info['name']] += 1

        for name, count in grouped_apps.items():
            running_apps.add_metric([self.cluster_name, name], count)
        yield running_apps


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("yarn_url", help="Yarn rest api address, eg: http://127.0.0.1:8080")
    parser.add_argument("--cluster-name", "-n", help="Yarn cluster name",
                        default="cluster_0")
    parser.add_argument("--port", "-p", help="Exporter listen port", type=int,
                        default=9459)
    parser.add_argument("--host", "-H", help="Exporter host address", default="0.0.0.0")
    parser.add_argument("--collected-apps", "-c", nargs="*",
                        help="Name of applications need to collect running status")
    parser.add_argument('--version', '-V', action='version', help="Show version info",
                        version=f'{__version__}')
    return parser


if __name__ == "__main__":
    args = get_parser().parse_args()

    REGISTRY.register(YarnClusterInfoCollector(args.yarn_url, args.cluster_name))
    REGISTRY.register(YarnMetricCollector(args.yarn_url, args.cluster_name))
    if args.collected_apps:
        REGISTRY.register(YarnRunningApplicationCollector(args.yarn_url, args.cluster_name, *args.collected_apps))
    app = make_wsgi_app(REGISTRY)
    httpd = make_server(args.host, args.port, app)
    httpd.serve_forever()
