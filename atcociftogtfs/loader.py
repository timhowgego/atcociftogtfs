"""loader is the front end for the processing class atcocif. loader main()
creates a single atcocif instance, walks through and sends a sequence of
ATCO-CIF files to the atcocif instance, and finally asks the instance write
an output GTFS archive."""


import argparse
import logging
import os
import urllib.request
import shutil
import tempfile
import time
import zipfile

from atcociftogtfs.atcocif import atcocif


def main(args=None):
    """Entry point: Start here. @param args namespace may contain processed
    arguments. If None, argparse will instead read command line arguments."""

    start_time = time.time()
    if args is None:
        args = arguments()

    if hasattr(args, "verbose") and args.verbose:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.WARNING

    if hasattr(args, "log_filename") and args.log_filename is not None:
        logging.basicConfig(
            filename=args.log_filename,
            level=logging_level,
            format="%(asctime)s:%(levelname)s:%(message)s",
        )
    else:
        logging.basicConfig(level=logging_level, format="%(message)s")

    processor = atcocif(args=args)  # Same class instance throughout

    if not hasattr(args, "source"):
        logging.error("No sources to process.")
        return 1

    logging.info("Gathering data from %s...", ", ".join(args.source))

    for source in args.source:
        processor = walk(source=source, processor=processor)

    if hasattr(args, "verbose") and args.verbose:
        processor.report(topic=None)

    if not hasattr(args, "gtfs_filename"):
        logging.error("No output file specified.")
        return 1

    status = processor.dump(filename=args.gtfs_filename)

    if status == 0:
        if processor.file_num > 1:
            logging.info(
                "Completed %s from %s ATCO-CIF files. Finished in %ss.",
                args.gtfs_filename,
                processor.file_num,
                round(time.time() - start_time),
            )
        else:
            logging.info(
                "Completed %s. Finished in %ss.",
                args.gtfs_filename,
                round(time.time() - start_time),
            )

    del processor

    return status


def arguments():
    """Parses @param arg command line arguments into raw @return args
    Namespace."""

    parser = argparse.ArgumentParser(
        description="Converts ATCO.CIF files into GTFS format.",
        prog="atcociftogtfs",
    )

    parser.add_argument(
        "source",
        nargs="+",
        help="""One or more ATCO.CIF data sources: directory, cif, url, zip
        (mixed sources, or sources containing a mixture, are fine).
        Required.""",
    )
    parser.add_argument(
        "-b",
        "--bank_holidays",
        nargs="?",
        dest="bank_holidays",
        help="""Filename (directory optional) for text file containing
        yyyymmdd bank (public) holidays, one per line. Optional, defaults to
        treating all days as non-holiday.""",
    )
    parser.add_argument(
        "-d",
        "--directional_routes",
        dest="directional_routes",
        action="store_true",
        help="""Uniquely identify inbound and outbound directions as
        different routes. Optional, defaults to combining inbound and
        outbound into the same route.""",
    )
    parser.add_argument(
        "-e",
        "--epsg",
        nargs="?",
        dest="epsg",
        type=int,
        help="""EPSG Geodetic Parameter Dataset code. For Ireland, 29903. For
        Great Britain, 27700. Optional, but GTFS stop lat and lng will be 0
        if argument is omitted.""",
    )
    parser.add_argument(
        "-f",
        "--final_date",
        nargs="?",
        dest="final_date",
        help="""Final yyyymmdd date of service, to replace ATCO-CIF's
        indefinite last date. Optional, defaults to conversion date +1
        year.""",
    )
    parser.add_argument(
        "-r",
        "--grid",
        nargs="?",
        dest="grid_figures",
        type=int,
        help="""Number of figures in each Northing or Easting grid reference
        value. ATCO-CIF should holds 8 figure grid references, but may
        contain less. Optional, defaults to best fit.""",
    )
    parser.add_argument(
        "-g",
        "--gtfs",
        nargs="?",
        default="gtfs.zip",
        dest="gtfs_filename",
        help="""Output GTFS zip filename (directory optional). Optional,
        defaults in gtfs.zip.""",
    )
    parser.add_argument(
        "-l",
        "--log",
        nargs="?",
        dest="log_filename",
        help="""Append feedback to this text filename (directory optional),
        not the console. Optional, defaults to console.""",
    )
    parser.add_argument(
        "-m",
        "--mode",
        nargs="?",
        default=3,
        dest="mode",
        type=int,
        help="""GTFS mode integer code. Optional, defaults to 3 (bus).""",
    )
    parser.add_argument(
        "-u",
        "--unique_ids",
        dest="unique_ids",
        action="store_true",
        help="""Force IDs for operators, routes and stops to be unique to
        each ATCO-CIF file processed within a multi-file batch. Safely
        reconciles files from different sources, but creates data
        redundancies within the resulting GTFS file. Optional, defaults to
        the identifiers used in the original ATCO-CIF files.""",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="""Verbose feedback of all progress to log or console. Optional,
        defaults to warnings and errors only.""",
    )
    parser.add_argument(
        "-s",
        "--school_term",
        nargs="?",
        dest="school_term",
        help="""Filename (directory optional) for text file containing
        yyyymmdd,yyyymmdd (startdate,enddate) school term periods, one
        comma-separated pair of dates per line. Optional, defaults to
        treating all periods as school term-time.""",
    )
    parser.add_argument(
        "-t",
        "--timezone",
        nargs="?",
        dest="timezone",
        default="Europe/London",
        help="""Timezone in IANA TZ format. Optional, defaults to
        Europe/London.""",
    )
    # Extendable: Add desc as atcocif var. Add desc to atcocif._arg_vars

    return parser.parse_args()


def walk(source, processor):
    """Walks/downloads/extracts @param source, where source is a directory,
    file, url, or zip (including mixed sources or sources containing a
    mixture), and @param processor is an existing atcocif instance, then
    initates ATCO-CIF processing. @return processor."""

    if os.path.isdir(source):
        for (root, dirs, files) in os.walk(source):
            for file in files:
                processor = walk(
                    source=os.path.join(root, file),
                    processor=processor
                )

    elif os.path.isfile(source):

        if zipfile.is_zipfile(source):
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip = zipfile.ZipFile(source)
                    zip.extractall(path=temp_dir)
                    zip.close()
                    processor = walk(
                        source=temp_dir,
                        processor=processor
                    )
            except Exception as e:
                logging.warning("Skipped %s: %s", source, e)

        else:
            status = processor.file(filename=source)
            if status == 0:
                logging.info("Completed %s", os.path.basename(source))

    else:

        try:
            request = urllib.request.Request(source)
            request_type = request.type

        except ValueError:
            request_type = None

        if request_type in ["http", "https"]:
            logging.info("Acquiring %s", source)  # URL implies delay

            try:
                with urllib.request.urlopen(request) as response:
                    # nosec - Filtered for non-http/https
                    with tempfile.NamedTemporaryFile(
                        delete=False
                    ) as temp_file:
                        # To temp, could be excessive for memory
                        shutil.copyfileobj(response, temp_file)
                        processor = walk(
                            source=temp_file.name,
                            processor=processor
                        )

            except Exception as e:
                logging.warning("Skipped %s: %s", source, e)

        else:
            logging.warning("Skipped missing/unhandleable source %s", source)

    return processor
