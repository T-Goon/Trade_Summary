# Trade Summary

A Python script to manipulate Excel and CSV files. Creates a summary and concise summary CSV files using the CSV files downloaded from different financial institutions.

Supports files from:
- Fidelity
- Etrade
- Sprot
- Ameritrade
- Canaccord
- Charles Schwab

**NOTE:** The CSV file formats from the financial institutions change over time. This script may need slight editing to account for the new formats.

## Usage

### Summary Maker
In the same directory put:
Summary_Maker.py

and folders named:

- 'Fidelity/'
- 'Etrade/'
- 'Sprot/'
- 'Ameritrade/'
- 'Canaccord/'
- 'Schwab/'

Put the input files into their corresponding folders and run the program.

**NOTE:**
- All rows in the in input files that have an empty quantity value are removed.
- Make sure that none of the input or output files are open in an editor when the
  program is run.
- All input files must be Excel or CSV files

### Concise Maker

Also makes a concise summary file that groups the data by symbol and agregates the cooresponding data.

**NOTE:**
- All assests without a symbol or quantity are not included in the file.
