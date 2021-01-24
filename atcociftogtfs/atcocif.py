"""atcocif contains a single class atcocif, which is initialised once, then
may receive 1 or more ATCO-CIF files to process, then may be finally
instructed to output a GTFS archive from the processed data."""


import csv
import datetime
import logging
import os
import urllib.parse
import sqlite3
import tempfile
import zipfile


class atcocif:
    """Initialise with @param args Namespace. Then core functions:
        file(@param filename) - ATCO-CIF filename to process
        report() - logs Quality Assurance summary of data processed
        dump(@param filename) - create a GTFS from processed data
    Maintain the same instance throughout (else unique IDs may duplicate).
    Del the instance to properly cleanup its sqlite database.
    """

    # -{ Data }---------------------------------------------------------------

    agency_cache = {}
    """               Agency data from the current file, pending processing:
                      agency_id: {name: str, phone: str}"""
    agency_used = []  # List of agency_id currently used in at 1+ trip/route
    base_filename = None  # Currently processing this filename, excluding path
    date_format = "%Y%m%d"  # Same for both ATCO-CIF and GTFS
    day_offset = 0  # Days after trip start (manages 25+ hour-clock times)
    bank_holidays = None  # List of datetimes (None = data missing)
    directional_routes = False  # Unique route_ids by direction
    epsg = None  # EPSG code (None = skip coordinate processing)
    file_num = 0  # Incrementing file counter
    final_date = None  # Final yyyymmdd date of service (default via __init__)
    grid_figures = None  # Northing/Easting grid ref figures (None = guess)
    gtfs_filename = None  # GTFS output zip filename (None = fail dump)
    in_trip = False  # Currently processing a trip_id
    last_hour = 0  # Hour of the last stop_time processed
    line_num = 0  # Incrementing file line counter
    mode = 3  # GTFS mode code (3 = bus)
    pre_times = False  # Currently processing trip pre-stop times sequence
    unique_ids = False  # Force unique IDs
    unsupported = {}  # Unsupported ATCO-CIF record ID: Count
    verbose = False  # Provide verbose feedback
    route_cache = {}
    """              Route data from the current file, pending processing:
                     route_id: {agency: id, num: str, inbound: str,
                         outbound: str}"""
    route_duplicate = []  # List of route_id found in multiple files
    route_used = []  # List of route_id currently used in at least 1 trip
    school_term = None  # List of datetimes (None = data missing)
    sequence = 0  # Incrementing stop sequence
    service = {}
    """          Calendar/calendar_dates entries, processed at EoF
                     service[trip_id]: {calendar: [calendar_list],
                     calendar_dates: [[calendar_exception_list], [...]]]}
                 As respective tables, except no initial service_id"""
    stop_cache = {}
    """             Stop data from the current file, pending processing:
                    stop_id: {name: str, easting: str, northing: str}"""
    stop_used = []  # List of stop_id currently used in at least 1 trip
    timezone = "Europe/London"  # IANA TZ
    trip_id = 0  # Incrementing trip_id

    _arg_vars = [
        "bank_holidays", "epsg", "directional_routes", "final_date",
        "grid_figures", "gtfs_filename", "mode", "unique_ids",
        "verbose", "school_term", "timezone"
    ]  # These variables can be overwritten by arguments of the same name

    _gtfs_structure = {
        "agency": {
            "agency_id": "TEXT",  # 4-Character Operator
            "agency_name": "TEXT",  # From QP
            "agency_url": "TEXT",  # As search
            "agency_timezone": "TEXT",  # Arg timezone
            "agency_phone": "TEXT",  # From QP
        },
        "stops": {
            "stop_id": "TEXT",  # 12-Character Location
            "stop_name": "TEXT",
            "stop_lat": "NUMERIC",  # Convert from ITM/etc Grid
            "stop_lon": "NUMERIC",  # Convert from ITM/etc Grid
        },
        "routes": {
            "route_id": "TEXT",  # 4-Character Operator + Route No
            "agency_id": "TEXT",
            "route_short_name": "TEXT",  # Route Num
            "route_long_name": "TEXT",  # From QD O+|+I
            "route_type": "INTEGER",  # Arg mode (Vehicle Type = non-standard)
        },
        "trips": {
            "route_id": "TEXT",
            "service_id": "INTEGER",  # Generated (references calendar)
            "trip_id": "INTEGER",  # Incrementing
            "trip_headsign": "TEXT",  # Journey Note
            "trip_short_name": "TEXT",  # Running Board
            "direction_id": "INTEGER",  # Route Direction, convert to 0/1
        },
        "stop_times": {
            "trip_id": "INTEGER",
            "arrival_time": "TEXT",  # Convert to HH:MM:SS
            "departure_time": "TEXT",  # Convert to HH:MM:SS
            "stop_id": "TEXT",
            "stop_sequence": "INTEGER",  # Generated
            "pickup_type": "INTEGER",  # Activity Flag, convert to 0/1
            "drop_off_type": "INTEGER",  # Activity Flag, convert to 0/1
            "timepoint": "INTEGER",  # Timing point indicator, convert to 0/1
        },
        "calendar": {
            # Test for match (including calendar_dates), else add new
            "service_id": "INTEGER",  # Incrementing
            "monday": "INTEGER",
            "tuesday": "INTEGER",
            "wednesday": "INTEGER",
            "thursday": "INTEGER",
            "friday": "INTEGER",
            "saturday": "INTEGER",
            "sunday": "INTEGER",
            "start_date": "TEXT",
            "end_date": "TEXT",
        },
        "calendar_dates": {
            # Used for Bank and School holiday exceptions
            "service_id": "INTEGER",
            "date": "TEXT",
            "exception_type": "INTEGER",  # 1 add, 2 remove
        },
    }
    """GTFS table: column: sqlite-type
       Structure minimalist: Required and used only, not full GTFS spec.
       Security Issue: Several functions loop through to define table or
       rows during sqlite query construction, and write the results in
       as strings, which would allow malicious injection, where the data
       not pre-defined here internally. While it is possible to hack in
       atcocif._gtfs_structure = {nastiness}, that is not accessible
       through arguments, thus only a local vulnerability."""

    # -{ Init }---------------------------------------------------------------

    def __del__(self):
        """Deconstructor (cleans up database)."""

        if hasattr(self, "db"):  # Else failed to __init__
            self.db.close()

    def __init__(self, args=None):
        """Initialise with @param args Namespace."""

        self.arguments(args=args)
        self.database(where="")

    def arguments(self, args=None):
        """Process @param args Namespace into internal values."""

        if args is not None:
            arguments = vars(args)

            for key, value in arguments.items():
                if key in self._arg_vars:

                    if key in ["bh", "stt"] and value is not None:
                        setattr(
                            atcocif,
                            key,
                            self.dates_from_file(filename=value)
                        )
                    else:
                        setattr(atcocif, key, value)

        if self.final_date is None:
            self.final_date = self.date_years_hence(years_hence=1).strftime(
                self.date_format
            )

    def database(self, where=""):
        """@return unpopulated GTFS-structured sqlite database object in
        @param where (by default, empty, so a temporary file that is primarily
        in memory but can use the hard drive if too large for memory)."""

        self.db = sqlite3.connect(where)
        c = self.db.cursor()

        for table, fields in self._gtfs_structure.items():
            sql_fields = []

            for field, type in fields.items():
                sql_fields.append("{} {}".format(field, type))

            c.execute("CREATE TABLE {} ({})".format(
                table, ", ".join(sql_fields)
            ))  # nosec - See _gtfs_structure Security Issue

        self.db.commit()

    def dates_from_file(self, filename=""):
        """Open comma-delimited text @param filename containing list of
        yyyymmdd dates or period pairs and @return array of all datetime
        days therein."""

        dates = []

        try:
            with open(filename, "r", newline="") as txt_file:
                reader = csv.reader(txt_file, delimiter=",")

                for line in reader:
                    if len(line) > 1:
                        start = datetime.datetime.strptime(
                            line[0],
                            self.date_format
                        )
                        end = datetime.datetime.strptime(
                            line[1],
                            self.date_format
                        )
                        delta = end - start

                        for day in range(delta.days + 1):
                            dates.append(start + datetime.timedelta(days=day))

                    elif len(line) > 0:
                        dates.append(
                            datetime.datetime.strptime(
                                line[0],
                                self.date_format
                            )
                        )

        except Exception as e:
            logging.error("Failed to import dates file %s: %s", filename, e)

        return dates

    def date_years_hence(self, years_hence=1):
        """@return datetime of a date @param integer years_hence from
        today."""

        today = datetime.date.today()

        try:
            return today.replace(year=today.year + years_hence)

        except ValueError:  # 29 February
            return today + (
                datetime.date(today.year + years_hence, 1, 1)
                - datetime.date(today.year, 1, 1)
            )

    # -{ Core }---------------------------------------------------------------

    def dump(self, filename=None):
        """Creates GTFS zip archive @param filename and writes in processed
        data, @return 1 OK or 0 not."""

        if filename is None:
            if self.gtfs_filename is None:
                return 1
            filename = self.gtfs_filename

        try:
            c = self.db.cursor()
            zip = zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED)

            with tempfile.TemporaryDirectory() as temp_dir:

                for table, fields in self._gtfs_structure.items():
                    arcname = "{}.txt".format(table)
                    path = os.path.join(temp_dir, arcname)

                    with open(
                        path, "w", newline="", encoding="utf-8"
                    ) as txtfile:
                        head_names = []

                        for field, type in fields.items():
                            head_names.append(field)

                        query = "SELECT {} FROM {}".format(
                            ", ".join(head_names),
                            table
                        )  # nosec - See _gtfs_structure Security Issue
                        c.execute(query)
                        txt = csv.writer(
                            txtfile, delimiter=",", quoting=csv.QUOTE_MINIMAL
                        )
                        txt.writerow(head_names)

                        line = c.fetchone()

                        while line:
                            txt.writerow(line)
                            line = c.fetchone()

                    zip.write(path, arcname)

            zip.close()
            return 0

        except Exception as e:
            logging.critical("Failed to write %s: %s", filename, e)
            return 1

    def file(self, filename=""):
        """The main function. Parses expected ATCO-CIF @param filename,
        processing the data therein. @return 0 if parsing suceeded (with no
        worse than warnings), 1 if erroneous (bad file type/strucrure or
        unrecoverable processing error."""

        self.agency_cache = {}
        self.agency_used = []
        self.file_num += 1
        self.line_num = 0
        self.route_used = []
        self.route_cache = {}
        self.service = {}
        self.stop_used = []
        self.stop_cache = {}

        try:
            self.base_filename = os.path.basename(filename)
            with open(filename, "r") as cif:
                line = cif.readline()

                while line:
                    self.line_num += 1

                    if self.line_num == 1:  # Header
                        if self.header(line=line) == 1:
                            return 1

                    elif len(line) >= 3:  # 2-char ID + 1+ of data

                        if (
                            line.startswith("QB")
                            or line.startswith("QL")
                        ):  # Stop Name/Grid
                            self.location(line=line)

                        elif line.startswith("QD"):  # Route Description
                            self.route_description(line=line)

                        elif line.startswith("QE"):  # Date Exceptions
                            self.date_exceptions(line=line)

                        elif (
                            line.startswith("QI")
                            or line.startswith("QO")
                            or line.startswith("QT")
                        ):
                            self.stop_times(line=line)  # Stop Times

                        elif (
                            line.startswith("QN")
                            or line.startswith("ZN")
                        ):  # Journey Note
                            self.journey_note(line=line)

                        elif line.startswith("QP"):  # Operator
                            self.operator(line=line)

                        elif line.startswith("QR"):  # Journey Repetition
                            self.repetition(line=line)

                        elif line.startswith("QS"):  # Journey Header
                            self.journey(line=line)

                        # Extendable for currently unsupported id

                        elif (
                            line.startswith("QQ")  # Operator address
                            or line.startswith("QV")  # Vehicle (bespoke)
                            or line.startswith("ZG")  # Service group
                            or line.startswith("ZJ")  # Operational detail
                        ):  # Unsupported by GTFS
                            pass

                        else:  # Unimplemented or unknown
                            id = line[:2].strip()
                            if len(id) > 0:
                                if id in self.unsupported:
                                    self.unsupported[id] += 1
                                else:
                                    self.unsupported[id] = 1

                    # End of line
                    line = cif.readline()

            # Post-file reading
            self.agency()
            self.calendar()
            self.route()
            self.stops()
            return 0

        except Exception as e:
            # Unforeseen error catchall, with detail for debugging
            logging.getLogger(__name__).exception(
                "Error processing line %s of %s: %s",
                self.line_num,
                os.path.basename(self.base_filename),
                e,
            )
            return 1

    def report(self, topic=None):
        """Logs Quality Assurance summary of data/quirks. All reports if
        @param topic is None, else topic must be one of 'coords',
        'duplication', 'unsupported', 'totals'."""

        c = self.db.cursor()

        if topic is None or topic == "totals":
            tables = ["agency", "routes", "stops", "trips"]
            output = []

            for table in tables:
                c.execute("SELECT COUNT(*) FROM {}".format(table))
                # nosec - Variables are hardcoded within function
                fetched = c.fetchone()
                if fetched is not None:
                    output.append("{} {}".format(fetched[0], table))

            logging.info("Records amassed: %s.", ", ".join(output))

        if topic is None or topic == "coords":
            c.execute(
                """SELECT COUNT(*) FROM stops WHERE stop_lat=? AND
                stop_lon=?""",
                (
                    0,
                    0,
                ),
            )
            fetched = c.fetchone()

            if fetched is not None and fetched[0] > 0:
                logging.info(
                    "{} {}".format(
                        "%s stop(s) have zero (0,0) coordinates.",
                        "Check %s's stops.txt for details."
                    ),
                    fetched[0],
                    self.gtfs_filename,
                )

        if topic is None or topic == "duplication":
            duplicate_count = len(self.route_duplicate)

            if duplicate_count > 0:
                self.route_duplicate.sort()
                output = []
                agency_id = None

                for route_id in self.route_duplicate:
                    split_route = route_id.split("_")
                    if len(split_route) == 2:
                        if split_route[0] != agency_id:
                            agency_id = split_route[0]
                            output.append("[{}] {}".format(
                                agency_id, split_route[1]
                            ))
                        else:
                            output.append(split_route[1])

                logging.info(
                    "{} {} {} {}".format(
                        "%s Route IDs were found in more than 1 ATCO-CIF",
                        "file. Ok if different source files are intended to",
                        "describe the same route. If not, consider argument",
                        "-u to avoid confusion ([agency ID] route): %s."
                    ),
                    duplicate_count,
                    ", ".join(output),
                )

        if topic is None or topic == "unsupported":
            unsupported_count = 0
            output = []

            explanation = {
                "OB": "AIM timing point detail, use unknown",
                "QA": "alternative stop location, use unknown",
                "QC": "stop clusters, unimplemented: stop parent",
                "QG": "interchange times, unimplemented: transfers",
                "QH": "bank holiday dates, overwritten by argument -b",
                "QJ": "interchange times, unimplemented: transfers",
                "QW": "interchange times, unimplemented: transfers",
                "QX": "route association, unimplemented: block",
                "QY": "journey association, unimplemented: block",
                "ZA": "AIM timing point detail, use unknown",
                "ZB": "AIM timing point detail, use unknown",
                "ZD": "AIM valid period, unimplemented: feed_start/end_date",
                "ZE": "AIM hail-and-ride, unimplemented: continuous_pickup",
                "ZL": "AIM stops including circulars, unimplemented: block",
                "ZS": "AIM reference, use unknown",
                "ZT": "AIM school term dates, overwritten by argument -s",
            }

            for id, count in self.unsupported.items():
                unsupported_count += count

                if id in explanation:
                    detail = explanation[id]
                else:
                    detail = "unknown"

                output.append("[{}] {} ({})".format(id, count, detail))

            if unsupported_count > 0:
                output.sort()
                logging.info(
                    "%s record(s) of unsupported ATCO-CIF type skipped: %s.",
                    unsupported_count,
                    ", ".join(output),
                )

    # -{ Record ID Processing }-----------------------------------------------

    def date_exceptions(self, line=""):
        """Processes date exception records in @param line."""

        if len(line) >= 19 and self.in_trip:

            start_date = self.sanitize_date(
                date_str=line[2:10],
                is_commence=True
            )
            end_date = self.sanitize_date(
                date_str=line[10:18],
                is_commence=False
            )
            if line[18] == "0":
                action = 2  # Remove
            else:
                action = 1  # Add

            if self.trip_id not in self.service:
                self.service[self.trip_id] = {}
            if "calendar_dates" in self.service[self.trip_id]:
                dates = self.service[self.trip_id]["calendar_dates"]
            else:
                dates = []
            if "calendar" in self.service[self.trip_id]:
                calendar = self.service[self.trip_id]["calendar"]
            else:
                calendar = []  # Malformed. Assume any day

            dates += self.calendar_exception_list(
                exception_dates=[],
                calendar=calendar,
                start_date=start_date,
                end_date=end_date,
                action=action,
                invert=True,
            )

            if len(dates) > 0:
                self.service[self.trip_id]["calendar_dates"] = (
                    self._unique_list(list=dates)
                )

    def header(self, line=""):
        """Checks for valid header in @param line. @return 0 if OK, 1 not."""

        if not isinstance(line, str) or len(line) < 10:
            logging.warning(
                "Unrecognised file type: Skipped %s",
                self.base_filename
            )
            return 1

        if not line.startswith("ATCO-CIF"):
            if line.startswith("HDTPS"):
                logging.warning(
                    "Non-ATCO-CIF file, likely railway CIF: Skipped %s",
                    self.base_filename
                )
            else:
                logging.warning(
                    "Non-ATCO-CIF file: Skipped %s",
                    self.base_filename
                )
            return 1

        if line[8:10] != "05":
            logging.warning(
                "Unsupported ATCO-CIF version %s: Skipped %s",
                line[8:10],
                self.base_filename,
            )
            return 1

        return 0

    def journey(self, line=""):
        """Processes journey header records in @param line."""

        if len(line) >= 3 and line[2] == "D":  # Deleted, so skip whole trip
            self.in_trip = False

        elif len(line) >= 65:
            agency_id = self.sanitize_id(
                id=line[3:7], allow_line_num=False, direction=0
            )
            route_num = line[38:42].strip()
            direction_id = self.direction_to_gtfs(id=line[64])
            route_id = self.sanitize_id(
                id="{}_{}".format(agency_id, route_num),
                allow_line_num=True,
                direction=direction_id
            )
            trip_short_name = line[42:48].strip()  # Running Board

            start_date = self.sanitize_date(
                date_str=line[13:21],
                is_commence=True
            )
            end_date = self.sanitize_date(
                date_str=line[21:29],
                is_commence=False
            )
            calendar = self.calendar_list(
                start_date=start_date,
                end_date=end_date,
                weekday_str=line[29:36]
            )
            calendar_dates = []

            if line[36] == "S":
                # School term time only: Remove inverse-stt
                if self.school_term is not None:
                    calendar_dates += self.calendar_exception_list(
                        exception_dates=self.school_term,
                        calendar=calendar,
                        start_date=start_date,
                        end_date=end_date,
                        action=2,
                        invert=True,
                    )
                # Else Default-only always include

            elif line[36] == "H":
                # School holiday only: Default-only ignore trip
                if self.school_term is None:
                    self.in_trip = False
                    return 0
                else:
                    # School holiday only: Remove stt
                    calendar_dates += self.calendar_exception_list(
                        exception_dates=self.school_term,
                        calendar=calendar,
                        start_date=start_date,
                        end_date=end_date,
                        action=2,
                        invert=False,
                    )

            if line[37] == "A":
                # Also on Bank Holidays: Add bh
                calendar_dates += self.calendar_exception_list(
                    exception_dates=self.bank_holidays,
                    calendar=[],
                    start_date=start_date,
                    end_date=end_date,
                    action=1,
                    invert=False,
                )

            elif line[37] == "B":
                # Bank holidays only: Default-only ignore trip
                if self.bank_holidays is None:
                    self.in_trip = False
                    return 0
                else:
                    # Bank holidays only: Add bh and then empty calendar
                    calendar_dates += self.calendar_exception_list(
                        exception_dates=self.bank_holidays,
                        calendar=[],
                        start_date=start_date,
                        end_date=end_date,
                        action=1,
                        invert=False,
                    )
                    calendar = self.calendar_list(
                        start_date=start_date,
                        end_date=end_date,
                        weekday_str=("0" * 7)
                    )

            elif line[37] == "X":
                # Except bank holidays. Remove bh
                calendar_dates += self.calendar_exception_list(
                    exception_dates=self.bank_holidays,
                    calendar=calendar,
                    start_date=start_date,
                    end_date=end_date,
                    action=2,
                    invert=False,
                )

            # From here onward, trip is confirmed to be included
            self.trip_id += 1
            self.in_trip = True
            self.pre_times = True

            if self.trip_id is not self.service:
                self.service[self.trip_id] = {}
            if len(calendar_dates) > 0:
                self.service[self.trip_id]["calendar_dates"] = (
                    self._unique_list(list=calendar_dates)
                )
            self.service[self.trip_id]["calendar"] = calendar
            # self.service processed at end of file, not here

            if agency_id not in self.agency_used:
                self.agency_used.append(agency_id)
            # Agency details are added via QP, not here

            if route_id not in self.route_used:
                self.route_used.append(route_id)
            if route_id not in self.route_cache:
                self.route_cache[route_id] = {}
            self.route_cache[route_id]["agency"] = agency_id
            self.route_cache[route_id]["num"] = route_num

            c = self.db.cursor()
            c.execute(
                """INSERT INTO trips (route_id, trip_id, trip_short_name,
                direction_id) VALUES (?,?,?,?)""",
                (
                    route_id,
                    self.trip_id,
                    trip_short_name,
                    direction_id,
                ),
            )  # service_id added at end of file, not here
            self.db.commit()

        else:
            self.in_trip = False  # Else trips may falsely be merged
            logging.warning(
                "Skipped trip due to malformed header at line %s of %s",
                self.line_num,
                self.base_filename,
            )

    def journey_note(self, line=""):
        """Processes journey notes in @param line. Journey-wide notes are
        processed into GTFS trip_headsign. The scope of ATCO-CIF notes is far
        less well defined that in GTFS trip_headsign, but since we cannot
        judge the nature of each ATCO-CIF note, all header notes are converted
        into a GTFS trip_headsign. ATCO-CIF may also add notes to individual
        stop times, which is not easily supported by GTFS
        (stop_times.stop_headsign is intended to change the headsign for all
        stops thereafter, which here would mean resetting headsign changes
        after each stop), so are currently ignored. The most common stop time
        notes are naturally handled by GTFS elsewhere - pickup/setdown."""

        if len(line) >= 8 and self.in_trip and self.pre_times:
            note = line[7:].strip()
            if note != "":
                c = self.db.cursor()

                c.execute(
                    """SELECT trip_headsign FROM trips WHERE trip_id=?""",
                    (self.trip_id,),
                )
                fetched = c.fetchone()
                if fetched[0] is not None:
                    note = "{} | {}".format(fetched[0], note)

                c.execute(
                    """UPDATE trips SET trip_headsign=? WHERE trip_id=?""",
                    (
                        note,
                        self.trip_id,
                    ),
                )
                self.db.commit()

    def location(self, line=""):
        """Processes location name or grid records in @param line. Data is
        held in self.stop_cache and only written to database at file end if
        in self.stop_used."""

        if len(line) >= 16 and line[2] != "D":
            stop_id = self.sanitize_id(
                id=line[3:15], allow_line_num=False, direction=0
            )
            if stop_id not in self.stop_cache:
                self.stop_cache[stop_id] = {}

            if line.startswith("QL"):
                if len(line) >= 64:  # Followed by Gazetteer extensions
                    stop_name = line[15:63].strip()
                else:
                    stop_name = line[15:].strip()
                if stop_name != "":
                    self.stop_cache[stop_id]["name"] = stop_name

            elif (
                line.startswith("QB")
                and len(line) >= 24
                and self.epsg is not None
            ):
                easting = line[15:23].strip()
                if len(line) >= 32:  # Followed by Gazetteer extensions
                    northing = line[23:31].strip()
                else:
                    northing = line[23:].strip()
                if easting != "" and northing != "":
                    self.stop_cache[stop_id]["easting"] = easting
                    self.stop_cache[stop_id]["northing"] = northing

    def operator(self, line=""):
        """Processes operator records in @param line. Data is held in
        self.agency_cache and only written to database at file end if in
        self.agency_used."""

        if len(line) >= 32 and line[2] != "D":
            agency_id = self.sanitize_id(
                id=line[3:7], allow_line_num=False, direction=0
            )
            if agency_id not in self.agency_cache:
                self.agency_cache[agency_id] = {}

            agency_name = line[7:31].strip()
            if agency_name != "":
                self.agency_cache[agency_id]["name"] = agency_name

            if len(line) >= 92:
                # Line may be truncated if phone numbers are empty
                agency_phone = line[91:].strip()
                if agency_phone != "":
                    self.agency_cache[agency_id]["phone"] = agency_phone

    def repetition(self, line=""):
        """Processes journey repetition records in @param line. The handling
        of these is hackish (reading the prior record back from the database,
        converting in and out of datetime), but adequate for a record line
        that is often not used in ATCO-CIF files."""

        if len(line) >= 31 and self.in_trip:
            c = self.db.cursor()
            prev_id = self.trip_id

            c.execute(
                """SELECT route_id, trip_headsign, direction_id FROM trips
                WHERE trip_id=?""",
                (prev_id,),
            )
            prev_trip = c.fetchone()

            c.execute(
                """SELECT arrival_time, departure_time, stop_id,
                stop_sequence, pickup_type, drop_off_type, timepoint FROM
                stop_times WHERE trip_id=? ORDER BY stop_sequence ASC""",
                (prev_id,),
            )
            prev_times = c.fetchall()

            if prev_trip is not None and prev_times is not None:
                self.trip_id += 1
                trip_short_name = line[24:30].strip()  # Running Board
                self.service[self.trip_id] = self.service[prev_id]

                c.execute(
                    """INSERT INTO trips (route_id, trip_id, trip_headsign,
                    trip_short_name, direction_id) VALUES (?,?,?,?,?)""",
                    (
                        prev_trip[0],
                        self.trip_id,
                        prev_trip[1],
                        trip_short_name,
                        prev_trip[2],
                    ),
                )

                # Resist urge to subsitiute datetime here (see 25+ clocks)
                departure_minutes = self._time_str_to_minutes(
                    time_str=line[14:18], is_gtfs=False
                )
                prev_time_minutes = self._time_str_to_minutes(
                    prev_times[0][1], is_gtfs=True
                )
                offset = departure_minutes - prev_time_minutes

                for stoptime in prev_times:
                    arrival_gtfs = self.time_tuple_to_gtfs_str(
                        time_tuple=(
                            divmod(
                                (
                                    self._time_str_to_minutes(
                                        time_str=stoptime[0], is_gtfs=True
                                    )
                                    + offset
                                ),
                                60,
                            )
                        )
                    )  # divmod(minutes, 60) = time_tuple (H, M,)
                    departure_gtfs = self.time_tuple_to_gtfs_str(
                        time_tuple=(
                            divmod(
                                (
                                    self._time_str_to_minutes(
                                        time_str=stoptime[1], is_gtfs=True
                                    )
                                    + offset
                                ),
                                60,
                            )
                        )
                    )

                    c.execute(
                        """INSERT INTO stop_times (trip_id, arrival_time,
                        departure_time, stop_id, stop_sequence, pickup_type,
                        drop_off_type, timepoint) VALUES (?,?,?,?,?,?,?,?)""",
                        (
                            self.trip_id,
                            arrival_gtfs,
                            departure_gtfs,
                            stoptime[2],
                            stoptime[3],
                            stoptime[4],
                            stoptime[5],
                            stoptime[6],
                        ),
                    )

                self.db.commit()

    def route_description(self, line=""):
        """Processes route descriptions in @param line. Data is held in
        self.route_cache and only written to database at file end if in
        self.route_used."""

        if len(line) >= 13 and line[2] != "D":
            agency_id = self.sanitize_id(
                id=line[3:7], allow_line_num=False, direction=0
            )
            if agency_id not in self.agency_used:
                self.agency_used.append(agency_id)
            """Agency details are added via QP, not here. Logic of adding
            here is safety first: Ideally added by QH, to mirror
            self.route_used. Here use is uncertain, merely logical. Risk of
            rogue agency causing GTFS quality warnings is in practice smaller
            than risk of missing a (malformed but relational) agency
            reference entirely."""

            route_num = line[7:11].strip()
            direction_id = self.direction_to_gtfs(id=line[11])
            route_id = self.sanitize_id(
                id="{}_{}".format(agency_id, route_num),
                allow_line_num=True,
                direction=direction_id
            )
            route_name = line[12:].strip()

            if route_id not in self.route_cache:
                self.route_cache[route_id] = {}
            # Any duplication defaults to last entry
            self.route_cache[route_id]["agency"] = agency_id
            self.route_cache[route_id]["num"] = route_num

            if route_name != "":
                if direction_id == 1:
                    self.route_cache[route_id]["inbound"] = route_name
                else:
                    self.route_cache[route_id]["outbound"] = route_name

    def stop_times(self, line=""):
        """Processes stop time records in @param line for @param id:
        QO (start), QI (intermediate), QT (end)."""

        if (
            (
                (len(line) >= 23 and not line.startswith("QI"))
                or (len(line) >= 28 and line.startswith("QI"))
            )
            and self.in_trip
            and (line.startswith("QO") or self.sequence > 0)
        ):
            stop_id = self.sanitize_id(
                id=line[2:14], allow_line_num=False, direction=0
            )
            if stop_id not in self.stop_used:
                self.stop_used.append(stop_id)
            # Stop details are added via QL and QB, not here

            arrival = self.time_str_to_time_tuple(
                time_str=line[14:18],
                is_gtfs=False
            )

            if line.startswith("QO"):
                self.pre_times = False
                self.sequence = 1
                self.last_hour = arrival[0]
                self.day_offset = 0
                # QUIRK: Offset uncertain if 24+ hours between stops
            else:
                self.sequence += 1
                if arrival[0] < self.last_hour:
                    self.day_offset += 1
                    # Recalculate arrival with new day_offset
                    arrival = self.time_str_to_time_tuple(
                        time_str=line[14:18], is_gtfs=False
                    )

            pickup = 0
            drop_off = 0
            timepoint = 0

            if line.startswith("QI"):
                departure = self.time_str_to_time_tuple(
                    time_str=line[18:22], is_gtfs=False
                )
                if departure[0] < self.last_hour:
                    self.day_offset += 1
                    # Recalculate departure with new day_offset
                    departure = self.time_str_to_time_tuple(
                        time_str=line[18:22], is_gtfs=False
                    )
                    self.last_hour = departure[0]
                if line[26:28] == "T1":
                    timepoint = 1
                if line[22] == "P":
                    drop_off = 1
                elif line[22] == "S":
                    pickup = 1
                elif line[22] == "N":
                    pickup = 1
                    drop_off = 1

            else:
                departure = arrival
                if line[21:23] == "T1":
                    timepoint = 1
                if line.startswith("QO"):
                    drop_off = 1
                elif line.startswith("QT"):
                    pickup = 1

            c = self.db.cursor()
            c.execute(
                """INSERT INTO stop_times (trip_id, arrival_time,
                    departure_time, stop_id, stop_sequence, pickup_type,
                    drop_off_type, timepoint) VALUES (?,?,?,?,?,?,?,?)""",
                (
                    self.trip_id,
                    self.time_tuple_to_gtfs_str(time_tuple=arrival),
                    self.time_tuple_to_gtfs_str(time_tuple=departure),
                    stop_id,
                    self.sequence,
                    pickup,
                    drop_off,
                    timepoint,
                ),
            )
            self.db.commit()

    # -{ End of File Processing }---------------------------------------------

    def agency(self):
        """Processes file's accumulated agency data, adding agency records to
        the database when in self.agency_used, using supporting data from
        self.agency_cache."""

        c = self.db.cursor()

        unknown_name = "Unknown Operator"
        insert = []
        update = []

        c.execute("""SELECT agency_id FROM agency""")
        known_agency_id = c.fetchall()

        c.execute(
            """SELECT agency_id FROM agency WHERE agency_name=?""",
            (unknown_name,)
        )
        known_empty_agency_id = c.fetchall()

        for agency_id in self.agency_used:

            # Defaults, used if operator records were missing
            agency_name = unknown_name
            agency_phone = None

            if (agency_id,) not in known_agency_id or (
                agency_id,
            ) in known_empty_agency_id:

                if agency_id in self.agency_cache:
                    if "name" in self.agency_cache[agency_id]:
                        agency_name = self.agency_cache[agency_id]["name"]
                    if "phone" in self.agency_cache[agency_id]:
                        agency_phone = self.agency_cache[agency_id]["phone"]

                # GTFS requires a URL, so spoofed usefully with a search
                agency_url = "https://www.google.com/search?q={}".format(
                    urllib.parse.quote_plus(agency_name)
                )

                if (agency_id,) not in known_agency_id:
                    insert.append(
                        (
                            agency_id,
                            agency_name,
                            agency_url,
                            self.timezone,
                            agency_phone,
                        )
                    )

                elif (
                    agency_name != unknown_name
                    and (agency_id,) in known_empty_agency_id
                ):
                    update.append(
                        (
                            agency_name,
                            agency_url,
                            agency_phone,
                            agency_id,
                        )
                    )

        if len(insert) > 0 or len(update) > 0:

            if len(insert) > 0:
                c.executemany(
                    """INSERT INTO agency (agency_id, agency_name,
                    agency_url, agency_timezone, agency_phone) VALUES
                    (?,?,?,?,?)""",
                    insert
                )

            if len(update) > 0:
                c.executemany(
                    """UPDATE agency SET agency_name=?, agency_url=?,
                    agency_phone=? WHERE agency_id=?""",
                    update
                )

            self.db.commit()

    def calendar(self):
        """Processes file's accumulated calendars, as held in self.service,
        to merge identical patterns together, write them into calendar and
        calendar_date tables, and update table trip service_id references."""

        c = self.db.cursor()
        unique = []  # Dict of unique self.service.trip_id entries
        trips = []  # Corresponding lists of trip_ids matching each unique

        for trip_id in self.service:
            seek = self.service[trip_id]
            if seek in unique:
                trips[unique.index(seek)].append(trip_id)
            else:
                unique.append(seek)
                trips.append([trip_id])

        for i in range(len(unique)):
            match_id = -1
            c.execute(
                """SELECT service_id FROM calendar WHERE monday=? AND
                tuesday=? AND wednesday=? AND thursday=? AND friday=? AND
                saturday=? AND sunday=? AND start_date=? AND end_date=?""",
                unique[i]["calendar"],
            )
            calendars = c.fetchall()  # 1+ may match, some with calendar_dates

            for result in calendars:
                c.execute(
                    """SELECT date, exception_type FROM
                    calendar_dates WHERE service_id=? ORDER BY date DESC,
                    exception_type DESC""",
                    result,
                )
                dates = c.fetchall()
                if (
                    "calendar_dates" in unique[i]
                    and unique[i]["calendar_dates"] == dates
                ) or ("calendar_dates" not in unique[i] and len(dates) == 0):
                    match_id = result[0]
                    break

            if match_id == -1:
                c.execute("""SELECT COUNT(*) FROM calendar""")
                match_id = 1 + (c.fetchone()[0])
                c.execute(
                    """INSERT INTO calendar (service_id, monday,
                    tuesday, wednesday, thursday, friday, saturday, sunday,
                    start_date, end_date) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    ([match_id] + unique[i]["calendar"]),
                )
                if "calendar_dates" in unique[i]:
                    for dates in unique[i]["calendar_dates"]:
                        c.execute(
                            """INSERT INTO calendar_dates (service_id,
                            date, exception_type) VALUES (?,?,?)""",
                            ([match_id] + dates),
                        )

            for trip_id in trips[i]:
                c.execute(
                    """UPDATE trips SET service_id=? WHERE trip_id=?""",
                    (
                        match_id,
                        trip_id,
                    ),
                )

            self.db.commit()

    def route(self):
        """Processes file's accumulated route data, adding self.route_cache to
        database where in self.route_used."""

        c = self.db.cursor()

        c.execute("""SELECT route_id FROM routes""")
        known_route_id = c.fetchall()

        c.execute(
            """SELECT route_id FROM routes WHERE route_long_name=?""",
            (None,)
        )
        known_empty_route_id = c.fetchall()

        insert = []
        update = []

        for route_id in self.route_used:

            if (
                (route_id,) not in known_route_id
                or (route_id,) in known_empty_route_id
            ):

                if route_id in self.route_cache:

                    if (
                        "inbound" in self.route_cache[route_id]
                        and "outbound" in self.route_cache[route_id]
                    ):
                        route_name = "{} | {}".format(
                            self.route_cache[route_id]["outbound"],
                            self.route_cache[route_id]["inbound"],
                        )
                    elif "inbound" in self.route_cache[route_id]:
                        route_name = self.route_cache[route_id]["inbound"]
                    elif "outbound" in self.route_cache[route_id]:
                        route_name = self.route_cache[route_id]["outbound"]
                    else:
                        route_name = None  # Valid empty if route num exists

                    if (route_id,) not in known_route_id:
                        insert.append(
                            (
                                route_id,
                                self.route_cache[route_id]["agency"],
                                self.route_cache[route_id]["num"],
                                route_name,
                                self.mode,
                            )
                        )

                    elif (
                        route_name is not None
                        and (route_id,) in known_empty_route_id
                    ):
                        update.append(
                            (
                                route_name,
                                route_id,
                            )
                        )

            else:
                if route_id not in self.route_duplicate:
                    self.route_duplicate.append(route_id)

        if len(insert) > 0 or len(update) > 0:

            if len(insert) > 0:
                c.executemany(
                    """INSERT INTO routes (route_id, agency_id,
                    route_short_name, route_long_name, route_type) VALUES
                    (?,?,?,?,?)""",
                    insert
                )

            if len(update) > 0:
                c.executemany(
                    """UPDATE routes SET route_long_name=? WHERE
                    route_id=?""",
                    update
                )

            self.db.commit()

    def stops(self):
        """Processes file's accumulated stop data, adding self.stop_cache to
        database where in self.stop_used."""

        c = self.db.cursor()

        out_of_bounds = 0  # Count of coordinates outside EPSG
        unknown_name = "Unknown"
        insert = []
        update = []

        c.execute("""SELECT stop_id from stops""")
        known_stop_id = c.fetchall()

        c.execute(
            """SELECT stop_id from stops WHERE stop_name=? OR
            (stop_lat=? AND stop_lon=?)""",
            (unknown_name, 0, 0),
        )
        known_empty_stop_id = c.fetchall()

        if self.epsg is not None:

            try:
                import pyproj

                transformer = pyproj.Transformer.from_crs(
                    "epsg:{}".format(self.epsg), "epsg:4326"
                )

            except ImportError:
                logging.warning(
                    "{} {}".format(
                        "Module pyproj required (pip install pyproj).",
                        "Meantime, skipping grid reference conversion."
                    )
                )
                self.epsg = None

            except pyproj.exceptions.CRSError:
                logging.warning(
                    "Invalid EPSG:%s. Skipping grid reference conversion.",
                    self.epsg
                )
                self.epsg = None

        for stop_id in self.stop_used:

            if (
                (stop_id,) not in known_stop_id
                or (stop_id,) in known_empty_stop_id
            ):

                # Defaults:
                stop_name = unknown_name
                stop_lat = 0
                stop_log = 0

                if stop_id in self.stop_cache:

                    if "name" in self.stop_cache[stop_id]:
                        stop_name = self.stop_cache[stop_id]["name"]

                    if (
                        self.epsg is not None
                        and "easting" in self.stop_cache[stop_id]
                        and "northing" in self.stop_cache[stop_id]
                    ):
                        if self.grid_figures is None:
                            # Assume accuracy of first applies to all
                            self.grid_figures = len(
                                self.stop_cache[stop_id]["easting"].strip()
                            )

                        latlog = transformer.transform(
                            self.sanitize_grid_ref(
                                ref=self.stop_cache[stop_id]["easting"]
                            ),
                            self.sanitize_grid_ref(
                                ref=self.stop_cache[stop_id]["northing"]
                            ),
                        )

                        if (
                            latlog[0] <= 90
                            and latlog[0] >= -90
                            and latlog[1] <= 180
                            and latlog[1] >= -180
                        ):  # Pyproj returns inf if out of bounds
                            stop_lat = round(latlog[0], 8)
                            stop_log = round(latlog[1], 8)
                        else:
                            out_of_bounds += 1

                if (stop_id,) not in known_stop_id:
                    insert.append(
                        (
                            stop_id,
                            stop_name,
                            stop_lat,
                            stop_log,
                        )
                    )

                elif ((stop_id,) in known_empty_stop_id and (
                    stop_name != unknown_name
                    or stop_lat != 0
                    or stop_log != 0)
                ):
                    update.append(
                        (
                            stop_name,
                            stop_lat,
                            stop_log,
                            stop_id,
                        )
                    )

        if out_of_bounds > 0:
            logging.warning(
                "{} {} {}".format(
                    "%s grid reference(s) were outside the EPSG:%s",
                    "boundary. Consider changing EPSG or forcing the",
                    "value of -r/--grid to not be %s."
                ),
                out_of_bounds,
                self.epsg,
                self.grid_figures,
            )

        if len(insert) > 0 or len(update) > 0:

            if len(insert) > 0:
                c.executemany(
                    """INSERT INTO stops (stop_id, stop_name, stop_lat,
                    stop_lon) VALUES (?,?,?,?)""",
                    insert
                )

            if len(update) > 0:
                c.executemany(
                    """UPDATE stops SET stop_name=?, stop_lat=?, stop_lon=?
                    WHERE stop_id=?""",
                    update
                )

            self.db.commit()

    # -{ Helpers }------------------------------------------------------------

    def _time_str_to_minutes(self, time_str="", is_gtfs=False):
        """@return integer minutes since notional midnight of @param time_str
        string in ATCO-CIF (HHMM), or if @param is_gtfs boolean True, GTFS
        (HH:MM:SS) format."""

        time_tuple = self.time_str_to_time_tuple(
            time_str=time_str,
            is_gtfs=is_gtfs
        )

        return (time_tuple[0] * 60) + time_tuple[1]

    def _unique_list(self, list=[]):
        """@return list consisting the unique parts of @param list."""

        unique = []
        last = []
        list.sort()

        for part in list:
            if part != last:
                unique.append(part)
                last = part

        return unique

    def calendar_exception_list(
        self,
        exception_dates=None,
        calendar=[],
        start_date="",
        end_date="",
        action=1,
        invert=False,
    ):
        """@return list of lists where each internal array consists [YYYYMMDD
        date, action], as required for self.service.trip_id.calendar_dates.
        @param exception_dates array of datetimes (as self.bank_holidays or
        self.school_term), or [] and invert=True to include all
        dates between start_date and end_date. @param calendar is as returned
        by self.calendar_list(), or [] for any day. @param start_date and
        end_date are YYYYMMDD strings. @param action integer is 1 to add
        dates, 2 to remove. @param invert boolean True to process the inverse
        of exception_dates (between start_date and end_date)."""

        if exception_dates is None:  # Bank holiday/school dates missing
            exception_dates = []

        try:
            dates = []
            check_dt = datetime.datetime.strptime(
                start_date,
                self.date_format
            )
            end_dt = datetime.datetime.strptime(end_date, self.date_format)
            step = datetime.timedelta(days=1)

            while check_dt <= end_dt:
                if (
                    (
                        (not invert and check_dt in exception_dates)
                        or (invert and check_dt not in exception_dates)
                    )
                    and (
                        len(calendar) == 0
                        or (calendar[check_dt.weekday()] == 1)
                    )
                ):
                    dates.append(
                        [check_dt.strftime(self.date_format), action]
                    )
                check_dt += step

            return dates

        except ValueError:
            logging.error(
                "Failed to create calendar_dates on line %s of %s",
                self.line_num,
                self.base_filename,
            )
            return []

    def calendar_list(self, start_date="", end_date="", weekday_str=""):
        """@return tuple as in as self.service.trip_id.calendar, which is
        as calendar table, except no initial service_id. @param start_date and
        @param end_date are ATCO-CIF/GTFS date strings. @param weekday_str is
        a 7-character string, consisting 0 or 1 for each day of the week from
        Monday."""

        try:
            return [
                int(weekday_str[0]),  # Monday
                int(weekday_str[1]),  # Tuesday
                int(weekday_str[2]),  # Wednesday
                int(weekday_str[3]),  # Thursday
                int(weekday_str[4]),  # Friday
                int(weekday_str[5]),  # Saturday
                int(weekday_str[6]),  # Sunday
                start_date,  # Start
                end_date,  # End
            ]

        except ValueError:
            logging.error(
                "Failed to create service calendar on line %s of %s",
                self.line_num,
                self.base_filename,
            )
            return ([0] * 7) + ([(" " * 8)] * 2)

    def direction_to_gtfs(self, id=""):
        """Converts ATCO-CIF direction string @param id into @return GTFS
        integer equivalent."""

        if id == "I":
            return 1  # Inbound

        return 0  # Outbound

    def sanitize_date(self, date_str="", is_commence=False):
        """Sanitize and @return ATCO-CIF or GTFS @param date_str (yyyymmdd).
        @param is_commence boolean True if date is th start date (only used to
        correct empty dates)."""

        try:
            if len(date_str) != 8:
                raise

            if date_str in [(" " * 8), ("9" * 8)]:
                if is_commence:
                    return datetime.date.today().strftime(self.date_format)
                return self.final_date

            return date_str

        except ValueError:
            logging.error(
                "Failed to sanitize date %s on line %s of %s",
                date_str,
                self.line_num,
                self.base_filename,
            )

            if is_commence:
                return datetime.date.today().strftime(self.date_format)
            return self.final_date

    def sanitize_grid_ref(self, ref=""):
        """Sanitize @param ref (single Easting or Northing) to @return
        float of equivalent of 6-figure grid reference."""

        try:
            return float("{}{}".format(
                ref.strip(),
                "0" * max(8 - self.grid_figures, 0)
            )) / 100

        except ValueError:
            return 0.0

    def sanitize_id(self, id="", allow_line_num=True, direction=0):
        """Sanitize @param id to add dummy id if empty and make unique if set
        in args, @return id. @param allow_line_num boolean makes dummy id
        unique to line (otherwise just unique to file), and should be set
        False only for agency and stop id (which become common to the whole
        file). @param direction int 0 unless route_id inbound direction 1."""

        id = id.strip()

        if len(id) == 0:
            # Create dummy ID
            if allow_line_num:
                id = "unknown_{}_{}".format(self.file_num, self.line_num)
            else:
                id = "unknown_{}".format(self.file_num)

        if self.directional_routes and direction == 1:
            id = "{}_inbd".format(id)

        if self.unique_ids:
            id = "{}_{:04d}".format(id, self.file_num)
            #  _ + 4-zerofill prevents duplication of any 4-figure base ID

        return id

    def time_str_to_time_tuple(self, time_str="", is_gtfs=False):
        """Converts @param time_str in ATCO-CIF (HHMM), or if @param is_gtfs
        boolean True, GTFS (HH:MM:SS) format to @return time_tuple
        (hour, minute,), adjusted for any active day_offset. Times are stored
        as tuples to more easily manage 25+ clock times, which confuse
        datetime functions. Seconds are always 0 because ATCO-CIF cannot hold
        seconds, so any created GTFS time must have 0 seconds."""

        try:
            if is_gtfs:
                hour = int(time_str[3:5])
            else:
                hour = int(time_str[2:4])
            minute = min(
                [
                    int(time_str[:2]) + (24 * self.day_offset),
                    99,  # Neither format allows >99 hours
                ]
            )
            return (
                minute,
                hour,
            )

        except ValueError:
            logging.error(
                "Failed to convert time %s on line %s of %s",
                time_str,
                self.line_num,
                self.base_filename,
            )
            return (
                0,
                0,
            )

    def time_tuple_to_gtfs_str(
        self,
        time_tuple=(
            0,
            0,
        ),
    ):
        """Converts @param time_tuple (hour, minute) into @return
        string GTFS time (HH:MM:SS)."""

        try:
            return "{:02d}:{:02d}:{:02d}".format(
                time_tuple[0], time_tuple[1], 0  # hour, minute, second
            )

        except ValueError:
            logging.error(
                "Failed to create GTFS time on line %s of %s",
                self.line_num,
                self.base_filename,
            )
            return "00:00:00"
