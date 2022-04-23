import numpy as np
import pandas as pd
import io
from os import listdir
from os.path import isfile, join
from datetime import date
from sys import argv

pathF = "Fidelity/"
pathE = "Etrade/"
pathS = "sprott/"
pathA = "Ameritrade/"
pathC = 'Canaccord/'

# pathF = argv[1]
# pathE = argv[2]
# pathS = argv[3]
# pathA = argv[4]


def main():

    # Create master DataFrame from columns from Fidelity files
    master = pd.DataFrame(columns=[
        "Account Name/Number",
        "Symbol",
        "Description",
        "Quantity",
        "Last Price",
        "Current Value",
        "Total Gain/Loss Dollar",
        "Total Gain/Loss Percent",
        "Cost Basis Per Share",
        "Total Cost Basis"
    ])

    # Parse the files and add to master
    master = parse_fidelity(master)
    master = parse_etrade(master)
    master = parse_sprott(master)
    master = parse_ameritrade(master)
    master = parse_canaccord(master)

    # Get rid of rows with no quantity
    master[master.columns[3]].replace('', np.nan, inplace=True)
    master.dropna(subset=[master.columns[3]], inplace=True)

    # Add names of banks at the end
    banks = pd.DataFrame(data={
        master.columns[0]: [  # Account Name
            "QCU", "Etrade", "VioBank", 'Dealmaker', 'Dealmaker', 'Dealmaker',
            'Dealmaker', 'Robin Hood', 'Robin Hood', 'Kraken'
        ],
        master.columns[1]: [  # Symbol
            "Cash", "Cash", "Cash", 'DTRC', 'Carbon Streaming', 'Carbon Streaming',
            'DTRC', 'BTC', 'Cash', 'BTC'
        ],
        master.columns[2]: [  # Description
            'Cash', 'Cash', 'Cash', 'JR Resources', 'Carbon Streaming', 'Carbon Streaming',
            'Dakota Territory Resource Corp', 'BTC', 'Cash', 'BTC'
        ],
        master.columns[4]:  # Last Price
        [
            '1', '1', '1'
        ] + ([''] * (10-3))
    })
    master = master.append(banks, ignore_index=True)

    # Export file as csv
    today = date.today()
    master.replace('[,]', '', regex=True, inplace=True)
    master.to_csv("Summary_Master_" + today.strftime("%b-%d-%Y") + ".csv", index=False)

# Parse the fidelity files and add them to the master


def parse_fidelity(master):
    # Find all of the files in the 'Fidelity' folder
    files = [join(pathF, f) for f in listdir(pathF) if isfile(join(pathF, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        file = pd.read_csv(io.StringIO(data), header=0, skipfooter=6, usecols=np.arange(0, 15),
                           na_values='--', engine='python', error_bad_lines=False)

        try:
            file = pd.read_excel(io.StringIO(data), header=0, skipfooter=6, usecols=np.arange(0, 15),
                           na_values='--')
        except:
            file = pd.read_csv(io.StringIO(data), header=0, skipfooter=6, usecols=np.arange(0, 15),
                           na_values='--', engine='python', error_bad_lines=False)

        # drop useless columns
        file = file.drop(columns=[
                         "Last Price Change", "Today's Gain/Loss Dollar", "Today's Gain/Loss Percent", "Type", "Percent Of Account"], errors='ignore')

        # Get rid of special characters
        file.replace('[-%]', '', regex=True, inplace=True)
        file[file.columns[4]].replace('[$]', '', regex=True, inplace=True)
        file[file.columns[5]].replace('[$]', '', regex=True, inplace=True)
        file[file.columns[6]].replace('[$\\+]', '', regex=True, inplace=True)
        file[file.columns[7]].replace('[%\\+]', '', regex=True, inplace=True)
        file[file.columns[8]].replace('[$]', '', regex=True, inplace=True)
        file[file.columns[9]].replace('[$]', '', regex=True, inplace=True)

        # Condense Account name and number columns into one
        if (master.columns[0] not in file.columns):
            file[master.columns[0]] = file[file.columns[0]] + " " + file[file.columns[1]]
            file = file.drop(columns=['Account Number', 'Account Name'])

        # Handle renamed Total Cost Basis column
        if (master.columns[9] not in file.columns and 'Cost Basis' in file.columns):
            file = file.rename(columns={'Cost Basis': master.columns[9]})

        master = master.append(file, ignore_index=True)

    return master

# Parse the etrade files and add them to the master
def parse_etrade(master):
    # Find all of the files in the 'Etrade' folder
    files = [join(pathE, f) for f in listdir(pathE) if isfile(join(pathE, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        try:
            fileTOP = pd.read_excel(io.StringIO(data), nrows=1, skiprows=[0])
            fileBOT = pd.read_excel(io.StringIO(data), skiprows=10, skipfooter=4, usecols=range(10))
        except:
            fileTOP = pd.read_csv(io.StringIO(data), nrows=1, skiprows=[0], engine='python', error_bad_lines=False)
            fileBOT = pd.read_csv(io.StringIO(data), skiprows=10, skipfooter=4, usecols=range(10), engine='python', error_bad_lines=False)

        # Get the account name from the top part of the csv
        name = fileTOP.at[0, fileTOP.columns[0]]
        del fileTOP

        # Change the format to be the same as the master
        temp = pd.DataFrame(data={
            master.columns[0]: [name for i in range(fileBOT.shape[0])],  # name
            master.columns[1]: fileBOT[fileBOT.columns[0]],  # Symbol
            master.columns[3]: fileBOT[fileBOT.columns[1]],  # Quantity
            master.columns[4]: fileBOT[fileBOT.columns[2]],  # Last Price
            master.columns[5]: fileBOT[fileBOT.columns[3]],  # Current Value
            # Total Gain/Loss Dollar
            master.columns[6]: fileBOT[fileBOT.columns[5]],
            # Total Gain/Loss Percent
            master.columns[7]: fileBOT[fileBOT.columns[7]],
            # Cost Basis Per Share
            master.columns[8]: fileBOT[fileBOT.columns[9]],
            # Calculate Cost Basis Total
            master.columns[9]: [
                p*q for p, q in zip(fileBOT[fileBOT.columns[9]], fileBOT[fileBOT.columns[1]])]
        })

        # append to master
        master = master.append(temp, ignore_index=True)

    return master

# Parse the sprott files and add them to the master
def parse_sprott(master):
    # Find all of the files in the 'Etrade' folder
    files = [join(pathS, f) for f in listdir(pathS) if isfile(join(pathS, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        try:
            fileTOP = pd.read_excel(io.StringIO(data), nrows=1, usecols=np.arange(0, 1))
            fileBOT = pd.read_excel(io.StringIO(data), skiprows=14, usecols=np.arange(0, 10))
        except:
            fileTOP = pd.read_csv(io.StringIO(data), nrows=1, usecols=np.arange(0, 1), engine='python', error_bad_lines=False)
            fileBOT = pd.read_csv(io.StringIO(data), skiprows=14, usecols=np.arange(0, 10), engine='python', error_bad_lines=False)

        # Get the name of the account
        name = fileTOP.at[0, fileTOP.columns[0]][9:26]
        del fileTOP

        # Change the format to be the same as the master
        temp = pd.DataFrame(data={
            master.columns[0]: [name for i in range(fileBOT.shape[0])],  # Name
            master.columns[1]: fileBOT[fileBOT.columns[1]],  # Symbol
            master.columns[2]: fileBOT[fileBOT.columns[0]],  # Description
            master.columns[3]: fileBOT[fileBOT.columns[2]],  # Quantity
            master.columns[4]: fileBOT[fileBOT.columns[3]],  # Last Price
            master.columns[5]: fileBOT[fileBOT.columns[4]],  # Current Value
            # Cost Basis Per Share
            master.columns[8]: fileBOT[fileBOT.columns[8]],
            master.columns[9]: fileBOT[fileBOT.columns[9]]  # Cost Basis Total
        })

        temp[master.columns[5]].replace('[\\*]', '', regex=True, inplace=True)

        # append to master
        master = master.append(temp, ignore_index=True)
    return master

# Parse the Ameritrade files and add them to the master


def parse_ameritrade(master):
    # Find all of the files in the 'Ameritrade' folder
    files = [join(pathA, f) for f in listdir(pathA) if isfile(join(pathA, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        try:
            file = pd.read_excel(io.StringIO(data), skipfooter=1)
        except:
            file = pd.read_csv(io.StringIO(data), skipfooter=1, engine='python', error_bad_lines=False)

        # Get the data into master format
        temp = pd.DataFrame(data={
            # name
            master.columns[0]: [f[len(f)-18:len(f)-5] for i in range(file.shape[0])],
            # Symbol
            master.columns[1]: file[file.columns[0]].str.findall("\\(([^\)]+)\\)").str[0],
            master.columns[2]: file[file.columns[0]],  # Description
            master.columns[3]: file[file.columns[1]],  # Quantity
            master.columns[4]: file[file.columns[6]],  # Last Price
            master.columns[5]: file[file.columns[7]],  # Current Value
            master.columns[6]: file[file.columns[8]],  # Total Gain/Loss Dollar
            # Total Gain/Loss Percent
            master.columns[7]: file[file.columns[9]],
            master.columns[8]: file[file.columns[3]],  # Cost Basis Per Share
            master.columns[9]: file[file.columns[4]]  # Cost Basis Total
        })

        # Add to master
        master = master.append(temp, ignore_index=True)

    return master


# Parse the Canaccord files and add them to the master
def parse_canaccord(master):
    # Find all of the files in the 'Canaccord' folder
    files = [join(pathC, f) for f in listdir(pathC) if isfile(join(pathC, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        try:
            file = pd.read_excel(io.StringIO(data), skiprows=2)
        except:
            file = pd.read_csv(io.StringIO(data), skiprows=2, engine='python', error_bad_lines=False)

        # Get the data into master format
        temp = pd.DataFrame(data={
            # name
            master.columns[0]: file[file.columns[2]],
            # Symbol
            master.columns[1]: file[file.columns[0]],
            master.columns[2]: file[file.columns[4]],  # Description
            master.columns[3]: file[file.columns[6]],  # Quantity
            master.columns[4]: file[file.columns[7]],  # Last Price
            master.columns[5]: file[file.columns[12]],  # Current Value
            master.columns[6]: ['']*file.shape[0],  # Total Gain/Loss Dollar
            # Total Gain/Loss Percent
            master.columns[7]: ['']*file.shape[0],
            master.columns[8]: ['']*file.shape[0],  # Cost Basis Per Share
            master.columns[9]: ['']*file.shape[0]  # Cost Basis Total
        })

        # Add to master
        master = master.append(temp, ignore_index=True)

    return master


if (__name__ == "__main__"):
    main()
