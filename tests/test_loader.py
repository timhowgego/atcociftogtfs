import tempfile
import types
import unittest

from atcociftogtfs.loader import main


class test_loader(unittest.TestCase):
    """Test loader (frontend)."""

    def test_main(self):
        """Test full application runtime loop."""

        with tempfile.NamedTemporaryFile(delete=False) as source:
            with open(source.name, "w") as cif_file:
                cif_file.write("ATCO-CIF0500")
            with tempfile.NamedTemporaryFile(delete=False) as gtfs:
                with tempfile.NamedTemporaryFile(delete=False) as log:
                    args = types.SimpleNamespace(
                        gtfs=gtfs.name,
                        log=log.name,
                        verbose=True,
                        source=[source.name],
                    )  # Log to silently tests verbose

                    self.assertEqual(main(args=args), 0)


if __name__ == "__main__":
    unittest.main()
