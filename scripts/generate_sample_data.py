#!/usr/bin/env python3
""" generate_sample_data.py

    Script to generate sample message archives for testing out data-digger.
"""

import argparse
import datetime
import functools
import gzip
import json
import logging
import multiprocessing
import os
import os.path
import random

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
)

ID_CHARS = list(
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')

# Data that we use to generate randomized messages / events
APPS = [
    {
        'name': 'amazing sports',
        'weight': 3.0,
        'event_types': [
            {
                'name': 'click link',
                'weight': 1.0,
                'latency': {
                    'mean': 50.0,
                    'stddev': 10.0,
                },
            },
            {
                'name': 'buy upgrade',
                'weight': 0.2,
                'latency': {
                    'mean': 150.0,
                    'stddev': 20.0,
                },
            },
            {
                'name': 'view page',
                'weight': 3.0,
                'latency': {
                    'mean': 5.0,
                    'stddev': 0.5,
                },
            },
        ],
        'oses': [
            {
                'name': 'windows',
                'weight': 0.2,
                'versions': [
                    {
                        'name': 'xp',
                        'weight': 1.0,
                    },
                    {
                        'name': '7',
                        'weight': 2.0,
                    },
                    {
                        'name': '10',
                        'weight': 10.0,
                    },
                ],
            },
            {
                'name': 'android',
                'weight': 10.0,
                'versions': [
                    {
                        'name': 'oreo',
                        'weight': 1.0,
                    },
                    {
                        'name': 'honeycomb',
                        'weight': 10.0,
                    },
                    {
                        'name': 'ice cream',
                        'weight': 1.0,
                    }
                ],
            },
            {
                'name': 'ios',
                'weight': 15.0,
                'versions': [
                    {
                        'name': '13.1',
                        'weight': 1.0,
                    },
                    {
                        'name': '13.2',
                        'weight': 2.0,
                    },
                    {
                        'name': '14.0',
                        'weight': 8.0,
                    },
                ],
            }
        ],
    },
    {
        'name': 'fun game',
        'weight': 1.0,
        'event_types': [
            {
                'name': 'start game',
                'weight': 1.0,
                'latency': {
                    'mean': 50.0,
                    'stddev': 10.0,
                },
            },
            {
                'name': 'buy upgrade',
                'weight': 2.0,
                'latency': {
                    'mean': 10.0,
                    'stddev': 0.5,
                },
            },
            {
                'name': 'view page',
                'weight': 3.0,
                'latency': {
                    'mean': 50.0,
                    'stddev': 10.0,
                },
            },
        ],
        'oses': [
            {
                'name': 'windows',
                'weight': 0.2,
                'versions': [
                    {
                        'name': 'xp',
                        'weight': 1.0,
                    },
                    {
                        'name': '7',
                        'weight': 2.0,
                    },
                    {
                        'name': '10',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'android',
                'weight': 10.0,
                'versions': [
                    {
                        'name': 'oreo',
                        'weight': 1.0,
                    },
                    {
                        'name': 'honeycomb',
                        'weight': 2.0,
                    },
                    {
                        'name': 'ice cream',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'ios',
                'weight': 15.0,
                'versions': [
                    {
                        'name': '13.1',
                        'weight': 1.0,
                    },
                    {
                        'name': '13.2',
                        'weight': 2.0,
                    },
                    {
                        'name': '14.0',
                        'weight': 8.0,
                    }
                ],
            }
        ]
    },
    {
        'name': 'cute pictures',
        'weight': 1.0,
        'event_types': [
            {
                'name': 'swipe picture',
                'weight': 1.0,
                'latency': {
                    'mean': 100.0,
                    'stddev': 10.0,
                },
            },
            {
                'name': 'open page',
                'weight': 2.0,
                'latency': {
                    'mean': 10.0,
                    'stddev': 2.0,
                },
            },
            {
                'name': 'see picture',
                'weight': 20.0,
                'latency': {
                    'mean': 20.0,
                    'stddev': 1.0,
                },
            },
        ],
        'oses': [
            {
                'name': 'windows',
                'weight': 5.0,
                'versions': [
                    {
                        'name': 'xp',
                        'weight': 1.0,
                    },
                    {
                        'name': '7',
                        'weight': 2.0,
                    },
                    {
                        'name': '10',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'android',
                'weight': 2.0,
                'versions': [
                    {
                        'name': 'oreo',
                        'weight': 1.0,
                    },
                    {
                        'name': 'honeycomb',
                        'weight': 2.0,
                    },
                    {
                        'name': 'ice cream',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'ios',
                'weight': 10.0,
                'versions': [
                    {
                        'name': '13.1',
                        'weight': 1.0,
                    },
                    {
                        'name': '13.2',
                        'weight': 2.0,
                    },
                    {
                        'name': '14.0',
                        'weight': 8.0,
                    }
                ],
            }
        ]
    },
    {
        'name': 'organize my life',
        'weight': 0.25,
        'event_types': [
            {
                'name': 'delete event',
                'weight': 5.0,
                'latency': {
                    'mean': 50.0,
                    'stddev': 5.0,
                },
            },
            {
                'name': 'open page',
                'weight': 10.0,
                'latency': {
                    'mean': 10.0,
                    'stddev': 2.0,
                },
            },
            {
                'name': 'create event',
                'weight': 5.0,
                'latency': {
                    'mean': 20.0,
                    'stddev': 3.0,
                },
            },
        ],
        'oses': [
            {
                'name': 'windows',
                'weight': 15.0,
                'versions': [
                    {
                        'name': 'xp',
                        'weight': 1.0,
                    },
                    {
                        'name': '7',
                        'weight': 2.0,
                    },
                    {
                        'name': '10',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'android',
                'weight': 15.0,
                'versions': [
                    {
                        'name': 'oreo',
                        'weight': 1.0,
                    },
                    {
                        'name': 'honeycomb',
                        'weight': 2.0,
                    },
                    {
                        'name': 'ice cream',
                        'weight': 10.0,
                    }
                ],
            },
            {
                'name': 'ios',
                'weight': 2.0,
                'versions': [
                    {
                        'name': '13.1',
                        'weight': 1.0,
                    },
                    {
                        'name': '13.2',
                        'weight': 2.0,
                    },
                    {
                        'name': '14.0',
                        'weight': 8.0,
                    }
                ],
            }
        ]
    },
]


def main():
    """Run the data generation."""
    args = get_args()

    os.makedirs(args.output_dir, exist_ok=True)

    worker_inputs = []
    total_messages = 0

    for i in range(args.num_files):
        output_file_path = os.path.join(
            args.output_dir,
            'archives_%02d.gz' % i,
        )
        message_count = int(args.messages_per_file * random.uniform(0.9, 1.1))
        worker_inputs.append(
            (
                output_file_path,
                message_count,
            )
        )
        total_messages += message_count

    pool = multiprocessing.Pool(args.num_workers)
    pool.map(
        generate_data_file,
        worker_inputs,
    )

    logging.info(
        'Sucessfully wrote %d messages across %d files',
        total_messages,
        args.num_files,
    )


def get_args():
    """Parse command-line args."""
    parser = argparse.ArgumentParser(
        description='Generate sample data')
    parser.add_argument(
        '--output-dir',
        type=str,
        default='test_inputs',
        help='Directory to dump outputs into',
    )
    parser.add_argument(
        '--num-files',
        type=int,
        default=20,
        help='Number of files to generate',
    )
    parser.add_argument(
        '--num-workers',
        type=int,
        default=10,
        help='Number of files to generate in parallel',
    )
    parser.add_argument(
        '--messages-per-file',
        type=int,
        default=60000,
        help='Approximate number of messages to include per file',
    )

    return parser.parse_args()


def generate_data_file(inputs):
    """Generate the date for a single file."""
    path, message_count = inputs

    logging.info(
        'Writing %d messages into %s',
        message_count,
        path,
    )

    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(days=10)

    with gzip.open(path, 'w') as output_file:
        for j in range(message_count):
            app = pick_item(APPS)
            os = pick_item(app['oses'])
            os_version = pick_item(os['versions'])
            event_type = pick_item(app['event_types'])
            message_id = random_str()
            latency = max(
                0.0,
                random.normalvariate(
                    event_type['latency']['mean'],
                    event_type['latency']['stddev'],
                ),
            )
            timestamp = random_time(start, end)

            event = {
                'app': app['name'],
                'context': {
                    'os': os['name'],
                    'version': os_version['name'],
                },
                'latency': latency,
                'messageId': message_id,
                'timestamp': timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'type': event_type['name'],
            }
            output_file.write(json.dumps(event).encode('utf-8'))
            if j < message_count - 1:
                output_file.write('\n'.encode('utf-8'))


def random_str(length=20):
    """Generate a random string of the argument length."""
    chars = []

    for i in range(length):
        chars.append(ID_CHARS[random.randint(0, len(ID_CHARS)-1)])

    return ''.join(chars)


def pick_item(items):
    """Do a weighted pick from a list of items."""
    weights = [item['weight'] for item in items]
    target = random.uniform(0, sum(weights))

    cum_total = 0.0

    for w, weight in enumerate(weights):
        cum_total += weight
        if target < cum_total:
            return items[w]


def random_time(start, end):
    """Generate a time chosen uniformly at random between the start and end."""
    delta = end - start
    delta_sec = delta.days * 86400 + delta.seconds
    return start + datetime.timedelta(seconds=random.uniform(0.0, delta_sec))


if __name__ == "__main__":
    main()
