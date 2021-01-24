# ATCO-CIF To GTFS

Converts ATCO.CIF (ATCO-CIF) public transport schedule files to [static GTFS format](https://gtfs.org/reference/static). ATCO (Association of Transport Coordinating Officers) CIF (Common Interface File) was the United Kingdom standard for bus schedule data transfer for the first decade of the 2000s, but has since been largely replaced by [TransXchange](https://www.gov.uk/government/collections/transxchange). ATCO-CIF differs from [the CIF format used by UK railways](https://wiki.openraildata.com/index.php/CIF_File_Format).

The converter supports ATCO.CIF version 5 (the only version ever deployed) but the current implementation focuses only on the core schedule/stop information that characterises most networks: There is no support for interchange (transfers), clustering (stop parents), journey associations (blocks), or most AIM data extensions (including hail-and-ride). By default, bank (public) holiday variations are ignored, and all dates are assumed to be in school term-time - but both assumptions can be overridden if the user provides bespoke lists of dates (via command line arguments `-b` and `-s`). Stop grid coordinate conversion is included, but the (EPSG) grid must be defined (via command line argument `-e`).

## Install

Install [Python 3](https://www.python.org/downloads/). Then (command prompt):

    pip install atcociftogtfs

## Getting Started

The most basic usage is from the command prompt:

    python -m atcociftogtfs

followed by one or more space-separated ATCO.CIF data sources (ATCO.CIF file, directory or zip file containing ATCO.CIF files, or internet URL of the same). By default, the converter will output a `gtfs.zip` to your current directory. 

If you do not understand the ATCO-CIF data you are importing, initially add two switches: `-u` (which protects against common _gotchas_, such as one bus operator with two identically numbered routes in different places) and `-v` (which gives feedback on processing and data).

To output comprehensive GTFS information you will need to specify `-b` (with a file listing bank holidays), `-e` (`29903` in Ireland, `27700` in Great Britain), and `-s` (with a file listing school term time periods) - all detailed below.

## Usage

Command prompt usage:

    python -m atcociftogtfs [optional arguments] source [source ...]
    
where `source` is one or more ATCO.CIF data sources: directory, cif, url, zip (mixed sources, or sources containing a mixture, are fine). Possible optional arguments:

* `-h`, `--help`: Show help.
* `-b [BANK_HOLIDAYS]`, `--bank_holidays [BANK_HOLIDAYS]`: Filename (directory optional) for text file containing `yyyymmdd` bank (public) holidays, one per line. Optional, defaults to treating all days as non-holiday.
* `-d`, `--directional_routes`: Uniquely identify inbound and outbound directions as different routes. Optional, defaults to combining inbound and outbound into the same route.
* `-e [EPSG]`, `--epsg [EPSG]`: EPSG Geodetic Parameter Dataset code. For Ireland, `29903`. For Great Britain, `27700`. Optional, but GTFS stop lat and lon will be 0 if argument is omitted.
* `-f [FINAL_DATE]`, `--final_date [FINAL_DATE]`: Final `yyyymmdd` date of service, to replace ATCO-CIF's indefinite last date. Optional, defaults to conversion date +1 year.
* `-r [GRID_FIGURES]`, `--grid [GRID_FIGURES]`: Number of figures in each Northing or Easting grid reference value. ATCO-CIF should holds 8-figure grid references, but may contain less. Optional, defaults to best guess.
* `-g [GTFS_FILENAME]`, `--gtfs [GTFS_FILENAME]`: Output GTFS zip filename (directory optional). Optional, defaults in `gtfs.zip`.
* `-l [LOG_FILENAME]`, `--log [LOG_FILENAME]`: Append feedback to this text filename (directory optional), not the console. Optional, defaults to console.
* `-m [MODE]`, `--mode [MODE]`: GTFS mode integer code. Optional, defaults to `3` (bus).
* `-u`, `--unique_ids`: Force IDs for operators, routes and stops to be unique to each ATCO-CIF file processed within a multi-file batch. Safely reconciles files from different sources, but creates data redundancies within the resulting GTFS file. Optional, defaults to the identifiers used in the original ATCO-CIF files.
* `-v`, `--verbose`: Verbose feedback of all progress to log or console. Optional, defaults to warnings and errors only.
* `-s [SCHOOL_TERM]`, `--school_term [SCHOOL_TERM]`: Filename (directory optional) for text file containing `yyyymmdd,yyyymmdd` (startdate,enddate) school term periods, one comma-separated pair of dates per line. Optional, defaults to treating all periods as school term-time.
* `-t [TIMEZONE]`, `--timezone [TIMEZONE]`: Timezone in IANA TZ format. Optional, defaults to `Europe/London`.

## Bugs and Contributions

Error reports and code improvements/extensions [are welcome](https://github.com/timhowgego/atcociftogtfs/issues). The current code should be functional, but is far from optimal. Please attach a copy of the relevant ATCO-CIF source file to reports about unexpected errors.
