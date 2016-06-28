# -*- coding: utf-8 -*-
#
# Copyright [2013, 2014, 2015] Lyft, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
A collectd plugin to write metrics to statsd.
"""
######################
# Standard Libraries #
######################
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import (dict, int, open, filter, zip)

#########################
# Third Party Libraries #
#########################
import collectd
import statsd

######################
# Internal Libraries #
######################


DEFAULTS = {
    'host': 'localhost',
    'port': 8125,
    'prefix': None,
    'typesdb': '/usr/share/collectd/types.db'
}


def parse_sources(sources):
    """
    Given a string of data sources; the second field in a line from a
    collectd types database, return a list of dictionaries describing those
    data sources.

    The sources should be delimited by spaces and, optionally, a comma
    (",") right after each list-entry.

    Each data-source is defined by a quadruple made up of the data-source
    name, type, minimal and maximal values, delimited by colons (":"):
    ds-name:ds-type:min:max. ds-type may be either ABSOLUTE, COUNTER,
    DERIVE, or GAUGE. min and max define the range of valid values for data
    stored for this data-source.
    """
    fields = ('name', 'type', 'min', 'max')
    sources = sources.replace(',', ' ').split()
    return [dict(zip(fields, source.split(':'))) for source in sources]


def parse_types(path):
    """
    Parse the file at PATH as a collectd types database, returning a
    dictionary of type information.

    Each line consists of two fields delimited by spaces and/or horizontal
    tabs. The first field defines the name of the data- set, while the
    second field defines a list of data-source specifications
    """
    with open(path) as types_db:
        # Split each line on the first run of whitespace. The first item of
        # each split is the name of the data-set. The second field is the
        # data-source specifications for that data set. We skip empty lines
        # and lines that start with '#' - these are comments.
        stripped = (line.strip() for line in types_db)
        specs = [line.split(None, 1) for line in stripped
                 if line and not line.startswith('#')]

    return dict((name, parse_sources(sources)) for name, sources in specs)


def configure(config, data=None):
    """
    Extract the statsd configuration data from the Config object passed in
    by collectd.
    """
    data = {
        'conf': DEFAULTS.copy(),
    }

    # The root node of the config is the Module block. The actual
    # configuration items are in `config.children`.
    for item in config.children:
        key = item.key.lower()

        # First, check if this is an expected configuration option.
        if key not in DEFAULTS:
            collectd.warning('Unexpected configuration key: %s!' % item.key)
            continue

        # Second, check only a single value was provided (we don't expect
        # any multiple-value configuration items, or items with no value).
        value_count = len(item.values)
        if value_count < 1:
            collectd.warning(
                'Must provide a value for configuration key: %s!' %
                item.key)
            continue
        elif value_count > 1:
            collectd.warning('Too many values for configuration key: %s!' %
                             item.key)
            collectd.warning('Expected 1, got %i' % value_count)
            continue

        # We've sanity-checked, so now we can use the value
        data['conf'][key] = item.values[0]

    data['types'] = parse_types(data['conf'].pop('typesdb'))
    collectd.register_init(initialize, data=data)


def initialize(data=None):
    """
    Create the statsd client object that will be used by the statsd_write
    function to send stats to statsd.

    This object will be shared between collectd threads, but because we are
    not using pipelines, the statsd object is thread safe.
    """
    data['stats'] = statsd.StatsClient(
        host=data['conf']['host'],
        port=int(data['conf']['port']),
        prefix=data['conf']['prefix']
    )
    collectd.register_write(statsd_write, data=data)


def stats_path(values):
    """
    Return the stats path for the given Values object.
    """
    # The filter() call here will remove any None values, so when we join()
    # these to get our stat path, only valid components will be joined.
    return '.'.join(filter(None, [
        # plugin name
        values.plugin,
        # plugin instance, if any
        getattr(values, 'plugin_instance', None),
        # type, if any
        getattr(values, 'type', None),
        # The name of the type instance
        values.type_instance,
    ]))


def write_interface(values, types, client=None):
    """
    Special handling for the interface plugin, because the path needs to
    include information from the type name.
    """
    # Strip the leading if_ from the type and append it to the path.
    path = '.'.join((stats_path(values), values.type[3:]))
    return write_stats(values, types, base_path=path, client=client)


def write_apache_worker_memory(values, types, client=None):
    """
    Special handling for the apache_worker_memory plugin because we want to
    send timers instead of gauges.
    """
    for idx, value in enumerate(values.values):
        path = '.'.join((values.plugin, values.plugin_instance))

        collectd.info('%s: %s = %s' % (values.plugin, path, value))

        if client is not None:
            # Intentionally *not* wrapped in a try/except so that an
            # exception here causes collectd to slow down trying to write
            # stats.
            client.timing(path, value)
        else:
            # No statsd client, be noisy
            message = 'Statsd client is None, not sending metrics!'
            collectd.warning(message)
            # Raise an exception so we aren't *too* noisy.
            raise RuntimeError(message)


def write_stats(values, types, base_path=None, client=None):
    """
    Actually write the stats to statsd!
    """
    for idx, value in enumerate(values.values):
        value = float(value)

        if base_path is None:
            base_path = stats_path(values)

        # Append the data source name, if any
        if len(values.values) > 1:
            path = '.'.join((base_path, types[values.type][idx]['name']))
        else:
            path = base_path

        collectd.info('%s: %s = %s' % (values.plugin, path, value))

        if client is not None:
            # Intentionally *not* wrapped in a try/except so that an
            # exception here causes collectd to slow down trying to write
            # stats.
            client.gauge(path, value)
        else:
            # No statsd client, be noisy
            message = 'Statsd client is None, not sending metrics!'
            collectd.warning(message)
            # Raise an exception so we aren't *too* noisy.
            raise RuntimeError(message)


def get_stats_writer(plugin):
    """
    Returns a writer function for the given plugin. If no custom writer
    function is defined, the default write_stats function is returned.
    """
    return globals().get('write_%s' % plugin, write_stats)


def statsd_write(values, data=None):
    """
    Entry point from collectd. Dispatches to a custom writer for the
    plugin, if one exists, or calls the default writer.
    """
    writer = get_stats_writer(values.plugin)
    return writer(values, data['types'], client=data['stats'])


collectd.register_config(configure)
