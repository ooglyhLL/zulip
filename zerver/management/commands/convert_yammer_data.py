
import argparse
import os
import tempfile
from typing import Any

from django.core.management.base import BaseCommand, CommandParser, CommandError

from zerver.data_import.yammer import do_convert_data

class Command(BaseCommand):
    help = """Convert the Yammer data into Zulip data format."""

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('yammer_data_zip', nargs='+',
                            metavar='<yammer data zip>',
                            help="Zipped yammer data")

        parser.add_argument('--output', dest='output_dir',
                            action="store", default=None,
                            help='Directory to write exported data to.')

        parser.add_argument('--threads',
                            dest='threads',
                            action="store",
                            default=6,
                            help='Threads to use in exporting UserMessage objects in parallel')

        parser.formatter_class = argparse.RawTextHelpFormatter

    def handle(self, *args: Any, **options: Any) -> None:
        output_dir = options["output_dir"]
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="converted-yammer-data-")
        else:
            output_dir = os.path.realpath(output_dir)

        num_threads = int(options['threads'])
        if num_threads < 1:
            raise CommandError('You must have at least one thread.')

        do_convert_data(options['yammer_data_zip'], output_dir, threads=num_threads)
