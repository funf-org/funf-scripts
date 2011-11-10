#!/usr/bin/env python
#
# Funf: Open Sensing Framework
# Copyright (C) 2010-2011 Nadav Aharony, Wei Pan, Alex Pentland.
# Acknowledgments: Alan Gardner
# Contact: nadav@media.mit.edu
# 
# This file is part of Funf.
# 
# Funf is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
# 
# Funf is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with Funf. If not, see <http://www.gnu.org/licenses/>.
# 

'''Analyzes funf data and reports on any gaps in data, by device, probe or both.
'''
from optparse import OptionParser
from collections import defaultdict
import sqlite3
import json
from datetime import datetime, timedelta
from itertools import takewhile, groupby, chain

_default_package = "edu.mit.media.funf.probe.builtin"
_default_pipeline = "edu.mit.media.funf.journal.MainPipeline"

def timestamps(conn, probe, device):
    cursor = conn.cursor()
    cursor.execute("select timestamp from data where device=? and probe=? order by timestamp asc",  (device, probe))
    return [x[0] for x in cursor.fetchall()]

def time_gaps(ts):
    return [cur - prev for prev, cur in zip([ts[0]] + ts[:-1], ts)]

def devices(conn):
    cursor = conn.cursor()
    cursor.execute("select distinct(device) from data order by device asc")
    return [x[0] for x in cursor.fetchall()]

def data_requests(configurations):
    return configurations["dataRequests"]


def histogram(values):
    counts = defaultdict(int)
    for v in values: counts[v] += 1
    return counts

def median(numbers):
    return sorted(numbers)[len(numbers)/2]


def configurations(conn, pipeline, device):
    cursor = conn.cursor()
    cursor.execute("select timestamp, value from data where probe=? and device=? order by timestamp asc",  (pipeline, device,))
    return [(x[0]/1000, json.loads(x[1])) for x in cursor.fetchall()]  # devide by a 100 due to a bug in ConfiguredPipeline

def removed_concecutive_duplicates(L, key=None):
    key = key or (lambda x: x);
    return [v for i, v in enumerate(L) if i == 0 or key(L[i]) != key(L[i-1])]

def get_conf_value(probe_conf, param_name):
    return probe_conf[0].get(param_name) if len(probe_conf) > 0 else None

def probe_period_changes(configurations, probe):
    probe_confs = [(time, conf["dataRequests"].get(probe)) for time, conf in configurations]
    probe_periods = [(time, get_conf_value(probe_conf, "PERIOD"), get_conf_value(probe_conf, "DURATION"))  for time, probe_conf in probe_confs if probe_conf]
    return removed_concecutive_duplicates(probe_periods, key=lambda x: x[1:])

def probe_period(configurations, probe, time):
    most_recent = None
    for conf_time, conf_value in sorted(configurations, key=lambda x: x[0]):
        if conf_time > time:
            break
        most_recent = conf_value
    
    conf = json.loads(most_recent)
    probe_conf = conf["dataRequests"].get(probe)
    return probe_conf[0].get("PERIOD") if len(probe_conf) > 0 else None

def data_gaps(probe_time_gaps, confs, probe, device):

    
    
    period_changes =  probe_period_changes(confs, probe)
    def get_period_change(value):
        return next(takewhile(lambda x: x[0] < value[0], reversed(period_changes)))
    
    expected_period_to_time_gaps = [(period_change, list(gaps)) 
                                    for period_change, gaps 
                                    in groupby(probe_time_gaps, get_period_change)]
    
    
    #possible_data_gaps = []
    definite_data_gaps = []
    for expected_period_to_time_gap in expected_period_to_time_gaps:
        expected_period = expected_period_to_time_gap[0][1] or 999999
        expected_duration = expected_period_to_time_gap[0][2] or 0
        tolerance = 120
        absolute_gaps = filter(lambda (time, time_gap): time_gap > expected_period + expected_duration + tolerance, expected_period_to_time_gap[1])
        #gaps_within_period = [(time, expected_period) for time, gap in absolute_gaps]
        #possible_data_gaps += gaps_within_period
        gaps_minus_period = [(time + expected_period, gap - expected_period) for time, gap in absolute_gaps]
        definite_data_gaps += gaps_minus_period
    return definite_data_gaps

def print_data_gaps(gaps):
    print "         h:mm:ss (from - to)"
    for timestamp, gap_seconds in gaps:
        duration = timedelta(seconds=gap_seconds)
        start_time = datetime.fromtimestamp(timestamp)
        end_time = datetime.fromtimestamp(timestamp + gap_seconds)
        print ("%s (%s - %s)" % (duration, start_time, end_time)).rjust(60)


def report(db_file, pipeline=None, probe=None, device=None, all_gaps=False):
    with sqlite3.connect(db_file) as conn:
        pipeline = pipeline or _default_pipeline
        funf_devices = [device] if device else devices(conn)
        for device in funf_devices:
            print
            print "=" * 100
            print "DEVICE: " + device
            confs = configurations(conn, pipeline, device)
            if probe and "." not in probe:
                probe = "%s.%s" % (_default_package, probe)
            probes = [probe] if probe else sorted(set([probe for probe in chain(*[conf["dataRequests"].keys() for time, conf in confs])]))
            
            for probe in probes:
                print
                print "-" * 100
                print "PROBE: %s" % probe
                
                ts = timestamps(conn, probe, device)
                if not ts:
                    print "No data found!"
                else:
                    probe_time_gaps = zip(ts, time_gaps(ts))
                    time_gap_values = [value for time,value in probe_time_gaps]
                        
                    print 
                    print "%d data entries" % len(ts)
                    start_datetime = datetime.fromtimestamp(ts[0])
                    end_datetime = datetime.fromtimestamp(ts[-1])
                    print "gathered over %s (%s - %s)" % ((end_datetime - start_datetime), start_datetime, end_datetime)
                    print
                    print "Time between scans"
                    print "Min: %s" % timedelta(seconds=min(time_gap_values))
                    print "Median: %s" % timedelta(seconds=median(time_gap_values))
                    print "Max: %s" % timedelta(seconds=max(time_gap_values))
                    
                    probe_data_gaps = data_gaps(probe_time_gaps, confs, probe, device)
                    if probe_data_gaps:
                        print 
                        print "Data gaps:"
                        print_data_gaps(probe_data_gaps)
                    else:
                        print 
                        print "NO Data Gaps"
                      
                    
                    if all_gaps:
                        counts = histogram(time_gap_values)
                        print
                        print "Time between scans"
                        print "h:mm:ss".rjust(16) + ": <count>"
                        for value in sorted(counts):
                            gap = timedelta(seconds=value)
                            print str(gap).rjust(16) + (": %d" % (counts[value],))
            

if __name__ == '__main__':
    usage = "%prog [options] data_file"
    description = "Decrypts files using the DES key specified, or the one included in this script.  Keeps a backup copy of the original file.  \nWARNING: This script does not detect if a file has already been decrypted.  \nDecrypting a file that is not encrypted will scramble the file."
    parser = OptionParser(usage="%s\n\n%s" % (usage, description))
    parser.add_option("-n", "--pipeline", dest="pipeline", default=None,
                      help="Limit the anaysis to a single pipeline. Use the full class name of pipeline.")
    parser.add_option("-p", "--probe", dest="probe", default=None,
                      help="Limit the anaysis to a single probe. Use the full class name, unless builtin probe.")
    parser.add_option("-d", "--device", dest="device", default=None,
                      help="Limit the anaysis to a single device. Use the full device UUID.")
    parser.add_option("-a", "--all", dest="all", default=False,
                      action="store_true",
                      help="Print out histogram of all data gaps.")
    
    (options, args) = parser.parse_args()
    
    if len(args)!=1:
        import sys
        sys.exit("Must specify exactly one db file as an argument.")

    report(args[0], options.pipeline, options.probe, options.device, options.all)
    