import datetime
import unittest
import tempfile

from atcociftogtfs.atcocif import atcocif


class test_atcocif(unittest.TestCase):
    """Test atcocif (ATCO-CIF file processing). Functions ordered in
    recommended runtime order, although each can be called alone."""

    processor = None  # Becomes atcocif() instance

    def setUp(self):
        """Initialise module atcocif."""

        self.processor = atcocif()

    def test_db(self):
        """Test initialisation of atcocif database."""

        self.assertTrue(hasattr(self.processor, "db"))

    def test_db_create(self):
        """Test database table creation."""

        c = self.processor.db.cursor()
        c.execute(
            """SELECT name FROM sqlite_master WHERE type=? AND name
            NOT LIKE ? ORDER BY name ASC""",
            (
                "table",
                "sqlite_%",
            ),
        )
        self.assertListEqual(
            c.fetchall(),
            [
                ("agency",),
                ("calendar",),
                ("calendar_dates",),
                ("routes",),
                ("stop_times",),
                ("stops",),
                ("trips",),
            ],
        )

    def test_holiday_import(self):
        """Test import of text file containing holiday dates."""

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with open(temp_file.name, "w") as txt_file:
                txt_file.write("20200228,20200301\n20200401\n")

            self.assertListEqual(
                self.processor.dates_from_file(filename=temp_file.name),
                [
                    datetime.datetime(2020, 2, 28, 0, 0),
                    datetime.datetime(2020, 2, 29, 0, 0),
                    datetime.datetime(2020, 3, 1, 0, 0),
                    datetime.datetime(2020, 4, 1, 0, 0),
                ],
            )

    def test_calendar_exception_school_days_only(self):
        """Test School Day Only Service."""

        self.assertListEqual(
            self.processor.calendar_exception_list(
                exception_dates=[
                    datetime.datetime(2020, 1, 6, 0, 0),
                    datetime.datetime(2020, 1, 7, 0, 0),
                    datetime.datetime(2020, 1, 8, 0, 0),
                ],  # School term time
                calendar=self.processor.calendar_list(
                    start_date="20200101",
                    end_date="20200112",
                    weekday_str="1010100"
                ),  # MWF only
                start_date="20200101",
                end_date="20200112",
                action=2,
                invert=True,
            ),
            [["20200101", 2], ["20200103", 2], ["20200110", 2]],
        )

    def test_calendar_exception_school_holiday_only(self):
        """Test School Holiday Only Service."""

        self.assertListEqual(
            self.processor.calendar_exception_list(
                exception_dates=[
                    datetime.datetime(2020, 1, 6, 0, 0),
                    datetime.datetime(2020, 1, 7, 0, 0),
                    datetime.datetime(2020, 1, 8, 0, 0),
                ],  # School term time
                calendar=self.processor.calendar_list(
                    start_date="20200101",
                    end_date="20200112",
                    weekday_str="1010100"
                ),  # MWF only
                start_date="20200101",
                end_date="20200112",
                action=2,
                invert=False,
            ),
            [["20200106", 2], ["20200108", 2]],
        )

    def test_calendar_exception_bank_holiday_only(self):
        """Test Bank Holiday Only Service."""

        self.assertListEqual(
            self.processor.calendar_exception_list(
                exception_dates=[
                    datetime.datetime(2020, 1, 1, 0, 0),
                    datetime.datetime(2020, 1, 2, 0, 0),
                ],  # Bank holidays
                calendar=[],
                start_date="20200101",
                end_date="20200112",
                action=1,
                invert=False,
            ),
            [["20200101", 1], ["20200102", 1]],
        )

    def test_line_date_exceptions(self):
        """Test ATCO-CIF QE line."""

        self.processor.in_trip = True

        self.processor.date_exceptions(line="QE20200101202001021")
        self.assertDictEqual(
            self.processor.service[self.processor.trip_id],
            {"calendar_dates": [["20200101", 1], ["20200102", 1]]},
        )

    def test_line_journey_trip(self):
        """Test ATCO-CIF QS line trip data."""

        self.processor.unique_ids = False
        self.processor.directional_routes = False
        self.processor.journey(
            line="{}{}".format(
                "QSNOP  42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )
        c = self.processor.db.cursor()
        c.execute(
            """SELECT route_id, trip_short_name, direction_id FROM trips
            WHERE trip_id=?""",
            (self.processor.trip_id,),
        )
        self.assertListEqual(c.fetchall(), [("OP_101", "101-42", 1)])

    def test_line_journey_calendar(self):
        """Test ATCO-CIF QS line calendar data."""

        self.processor.journey(
            line="{}{}".format(
                "QSNOP1 42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )
        self.assertDictEqual(
            self.processor.service[self.processor.trip_id],
            {"calendar": [1, 0, 1, 0, 1, 0, 0, "20200101", "20200112"]},
        )

    def test_journey_note(self):
        """Test ATCO-CIF QN line calendar data."""

        self.processor.journey(
            line="{}{}".format(
                "QSNOP  42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )

        self.processor.journey_note(line="QNA      Via Aplace  ")
        c = self.processor.db.cursor()
        c.execute(
            """SELECT trip_headsign FROM trips WHERE trip_id=?""",
            (self.processor.trip_id,),
        )
        self.assertListEqual(c.fetchall(), [("Via Aplace",)])

    def test_location_name(self):
        """Test ATCO-CIF QL line."""

        self.processor.unique_ids = False
        self.processor.location(line="QLNSTOP-REF0001The Stop    ")
        self.assertEqual(
            self.processor.stop_cache["STOP-REF0001"]["name"],
            "The Stop"
        )

    def test_location_grid(self):
        """Test ATCO-CIF QB line."""

        self.processor.unique_ids = False
        self.processor.epsg = 29903
        self.processor.location(line="QBNSTOP-REF0002333448  373764")
        self.assertDictEqual(
            self.processor.stop_cache["STOP-REF0002"],
            {"easting": "333448", "northing": "373764"},
        )

    def test_operator(self):
        """Test ATCO-CIF QP line."""

        self.processor.unique_ids = False
        self.processor.directional_routes = False
        self.processor.operator(
            line="{}{}".format(
                "QPNOP2 Operator Two            Operator Two Ltd    ",
                "                            118500      08712002233 ",
            )
        )
        self.assertDictEqual(
            self.processor.agency_cache["OP2"],
            {"name": "Operator Two", "phone": "08712002233"},
        )

    def test_stop_times(self):
        """Test ATCO-CIF QO/QI/QT lines."""

        self.processor.journey(
            line="{}{}".format(
                "QSNOP  42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )

        self.processor.stop_times(line="QOSTOP-REF00032215A  T1F1")
        self.processor.stop_times(line="QISTOP-REF000422552300B   T0F0")
        self.processor.stop_times(line="QISTOP-REF000523402341P   T0F0")
        self.processor.stop_times(line="QISTOP-REF000623502350S   T1F0")
        self.processor.stop_times(line="QTSTOP-REF00070025A  T1F0")
        c = self.processor.db.cursor()
        c.execute(
            """SELECT arrival_time, departure_time, stop_id, stop_sequence,
            pickup_type, drop_off_type, timepoint FROM stop_times WHERE
            trip_id=? ORDER BY stop_sequence ASC""",
            (self.processor.trip_id,),
        )
        self.assertListEqual(
            c.fetchall(),
            [
                ("22:15:00", "22:15:00", "STOP-REF0003", 1, 0, 1, 1),
                ("22:55:00", "23:00:00", "STOP-REF0004", 2, 0, 0, 0),
                ("23:40:00", "23:41:00", "STOP-REF0005", 3, 0, 1, 0),
                ("23:50:00", "23:50:00", "STOP-REF0006", 4, 1, 0, 1),
                ("24:25:00", "24:25:00", "STOP-REF0007", 5, 1, 0, 1),
            ],
        )

    def test_repetition(self):
        """Test ATCO-CIF QR line."""

        self.processor.journey(
            line="{}{}".format(
                "QSNOP  42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )
        self.processor.stop_times(line="QOSTOP-REF00082315A  T1F1")
        self.processor.stop_times(line="QTSTOP-REF00092355A  T1F0")

        self.processor.repetition(
            line="QRSTOP-REF0008234543    101-43BIGBUS  "
        )
        c = self.processor.db.cursor()
        c.execute(
            """SELECT arrival_time, departure_time, stop_id, stop_sequence,
            pickup_type, drop_off_type, timepoint FROM stop_times WHERE
            trip_id=? ORDER BY stop_sequence ASC""",
            (self.processor.trip_id,),
        )
        self.assertListEqual(
            c.fetchall(),
            [
                ("23:45:00", "23:45:00", "STOP-REF0008", 1, 0, 1, 1),
                ("24:25:00", "24:25:00", "STOP-REF0009", 2, 1, 0, 1),
            ],
        )

    def test_route_description(self):
        """Test ATCO-CIF QD line."""

        self.processor.unique_ids = False
        self.processor.directional_routes = False
        self.processor.route_description(line="QDNOP3 45A OCity - Town ")
        self.assertDictEqual(
            self.processor.route_cache["OP3_45A"],
            {"agency": "OP3", "num": "45A", "outbound": "City - Town"},
        )

    def test_agency(self):
        """Test agency processing."""

        self.processor.unique_ids = False
        self.processor.directional_routes = False
        self.processor.operator(
            line="{}{}".format(
                "QPNOP4 Operator Four           Operator Four Ltd   ",
                "                            118500      08712002233 ",
            )
        )
        self.processor.journey(
            line="{}{}".format(
                "QSNOP4 42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )  # Operator has to operate something

        self.processor.agency()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT agency_name, agency_phone FROM agency WHERE
            agency_id=?""",
            ("OP4",),
        )
        self.assertListEqual(c.fetchall(), [("Operator Four", "08712002233")])

    def test_calendar_trips(self):
        """Test calendar processing service_id into trips."""

        self.processor.service = {}  # Clear any prior tests
        self.processor.journey(
            line="{}{}".format(
                "QSNOP5 42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )

        self.processor.calendar()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT service_id FROM trips WHERE trip_id=?""",
            (self.processor.trip_id,),
        )
        self.assertNotEqual(
            c.fetchone()[0], None
        )  # None should be filled by calendar()

    def test_calendar(self):
        """Test calendar processing into calendar."""

        self.processor.service = {}  # Clear any prior tests
        self.processor.journey(
            line="{}{}".format(
                "QSNOP6 42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )

        self.processor.calendar()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT service_id FROM trips WHERE trip_id=?""",
            (self.processor.trip_id,),
        )
        service_id = c.fetchone()[0]
        c.execute(
            """SELECT monday, tuesday, wednesday, thursday, friday, saturday,
            sunday, start_date, end_date FROM calendar WHERE service_id=?""",
            (service_id,),
        )
        self.assertListEqual(
            c.fetchall(), [(1, 0, 1, 0, 1, 0, 0, "20200101", "20200112")]
        )

    def test_calendar_dates(self):
        """Test calendar processing into calendar_dates."""

        self.processor.service = {}  # Clear any prior tests
        self.processor.journey(
            line="{}{}".format(
                "QSNOP7 46    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )
        self.processor.date_exceptions(line="QE20200101202001030")

        self.processor.calendar()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT service_id FROM trips WHERE trip_id=?""",
            (self.processor.trip_id,),
        )
        service_id = c.fetchone()[0]
        c.execute(
            """SELECT date, exception_type FROM calendar_dates WHERE
            service_id=? ORDER BY date ASC""",
            (service_id,),
        )
        self.assertListEqual(
            c.fetchall(), [("20200101", 2), ("20200103", 2)]
        )  # Never operates thursday, so no 2 January 2020 removal

    def test_route(self):
        """Test route processing."""

        self.processor.unique_ids = False
        self.processor.directional_routes = False
        self.processor.route_cache = {}  # Clear any prior tests
        self.processor.route_description(line="QDNOP8 102 OCity - Town ")
        self.processor.route_description(line="QDNOP8 102 ITown - City")
        self.processor.journey(
            line="{}{}".format(
                "QSNOP8 48    2020010120200112",
                "1010100  102 101-42BIGBUS  TC=10142I"
            )
        )  # Route must have a trip (in any direction)

        self.processor.route()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT agency_id, route_short_name, route_long_name FROM
            routes WHERE route_id=?""",
            ("OP8_102",),
        )
        self.assertListEqual(
            c.fetchall(), [("OP8", "102", "City - Town | Town - City")]
        )

    def test_stops(self):
        """Test stops processing (without coordinates)."""

        self.processor.unique_ids = False
        self.processor.stop_cache = {}  # Clear any prior tests
        self.processor.location(line="QLNSTOP-REF0010Bus Stop")
        self.processor.journey(
            line="{}{}".format(
                "QSNOP9 42    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )  # Stop must have been used...
        self.processor.stop_times(line="QOSTOP-REF00102315A  T1F1")

        self.processor.stops()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT stop_name FROM stops WHERE stop_id=?""",
            ("STOP-REF0010",)
        )
        self.assertListEqual(c.fetchall(), [("Bus Stop",)])

    def test_stops_coordinates(self):
        """Test stops processing (with coordinates)."""

        self.processor.unique_ids = False
        self.processor.epsg = 29903
        self.processor.stop_cache = {}  # Clear any prior tests
        self.processor.location(line="QBNSTOP-REF0011333448  373764")
        self.processor.journey(
            line="{}{}".format(
                "QSNOP1042    2020010120200112",
                "1010100  101 101-42BIGBUS  TC=10142I"
            )
        )  # Stop must have been used...
        self.processor.stop_times(line="QOSTOP-REF00112315A  T1F1")

        self.processor.stops()
        c = self.processor.db.cursor()
        c.execute(
            """SELECT stop_lat, stop_lon FROM stops WHERE stop_id=?""",
            ("STOP-REF0011",),
        )
        self.assertListEqual(c.fetchall(), [(54.59449625, -5.93612739)])


if __name__ == "__main__":
    unittest.main()
