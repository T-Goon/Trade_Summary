import numpy as np
import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import date
import re
from sys import argv

pathF = "Fidelity\\"
pathE = "Etrade\\"
pathS = "Sprot\\"
pathA = "Ameritrade\\"

# pathF = argv[1]
# pathE = argv[2]
# pathS = argv[3]
# pathA = argv[4]

def main():

    # Create master DataFrame from columns from Fidelity files
    master = pd.DataFrame(columns = [
    "Account Name/Number",
    "Symbol",
    "Description",
    "Quantity",
    "Last Price",
    "Current Value",
    "Total Gain/Loss Dollar",
    "Total Gain/Loss Percent",
    "Cost Basis Per Share",
    "Cost Basis Total"
    ])

    # Parse the files and add to master
    master = parse_fidelity(master)
    master = parse_etrade(master)
    master = parse_sprot(master)
    master = parse_ameritrade(master)

    # Get rid of rows with no quantity
    master[master.columns[3]].replace('', np.nan, inplace=True)
    master.dropna(subset=[master.columns[3]], inplace=True)

    # Add names of banks at the end
    banks = pd.DataFrame(data = {master.columns[0]: ["QCU", "Etrade", "VioBank"]})
    master = master.append(banks, ignore_index=True)

    # Export file as csv
    today = date.today()
    master.to_csv("Summary_" + today.strftime("%b-%d-%Y") + ".csv")

    # create concise summary file
    concise = pd.DataFrame(columns = [
    master.columns[1], # Symbol
    master.columns[3], # Quantity
    master.columns[4], # Last Price
    master.columns[5], # Current Value
    master.columns[6], # Total Gain/Loss Dollar
    master.columns[7], # Total Gain/Loss Percent
    "Cost Basis Average",
    master.columns[9], # Cost Basis Total
    "Position Size(%)"
    ])

    concise = create_concise(concise, master)

    concise.to_csv("Concise_" + today.strftime("%b-%d-%Y") + ".csv")

# creates the concise DataFrame from the master DateFrame
def create_concise(concise, master):
    # Get unique stock symbols
    symbols = master.Symbol.unique()
    # if there is no symbol get rid of it
    symbols = [symbols[i] for i in range(len(symbols)) if type(symbols[i]) is str]
    # get rid of all symbols that contain numbers
    symbols = [symbols[i] for i in range(len(symbols)) if not any(char.isdigit() for char in symbols[i])]

    # total value of all positions
    totalValue = pd.to_numeric(master[master.columns[5]]).sum()

    for i in range(len(symbols)):
        # Calculate values for each stock
        # sum the quantity
        quan = float(master.loc[master[master.columns[1]] == symbols[i]][master.columns[3]].sum())

        # find the min last price if there is more than one
        lpIndex = pd.to_numeric(master.loc[master[master.columns[1]] == symbols[i]][master.columns[4]]).idxmin()
        if type(lpIndex) is float:
            lp = "n/a"
        else:
            lp = master.at[int(lpIndex), master.columns[4]]

        # sum the value of each position
        val = float(pd.to_numeric(master.loc[master[master.columns[1]] == symbols[i]][master.columns[5]]).sum())

        # sum total cost basis
        cbt = float(pd.to_numeric(master.loc[master[master.columns[1]] == symbols[i]][master.columns[9]]).sum())
        # cost basis average
        cba = cbt/quan

        tgld = val - cbt
        if(cbt == 0):
            tglp = "n/a"
        else:
            tglp = ((val - cbt)/cbt) * 100

        ps = (val/totalValue) * 100

        # add to concise
        temp = pd.DataFrame(data = {
        master.columns[1] : [symbols[i]], # Symbol
        master.columns[3] : [quan], # Quantity
        master.columns[4] : [lp], # Last Price
        master.columns[5] : [val], # Current Value
        master.columns[6] : [tgld], # Total Gain/Loss Dollar
        master.columns[7] : [tglp], # Total Gain/Loss Percent
        concise.columns[6] : [cba], # Cost Basis Average
        master.columns[9] : [cbt], # Cost Basis Total
        concise.columns[8] : [ps]
        })

        concise = concise.append(temp, ignore_index=True)

    return concise

# Parse the fidelity files and add them to the master
def parse_fidelity(master):
    # Find all of the files in the 'Fidelity' folder
    files = [join(pathF, f) for f in listdir(pathF) if isfile(join(pathF, f))]

    # append all of the files to masters
    for f in files:
        file = pd.read_csv(f, header = 0, skipfooter = 6, usecols = np.arange(0,14),
            na_values='--')

        # drop useless columns
        file = file.drop(columns=["Last Price Change", "Today's Gain/Loss Dollar", "Today's Gain/Loss Percent", "Type"])

        # Get rid of special characters
        file.Symbol.replace('[ -]', '', regex=True, inplace = True)
        file[file.columns[4]].replace('[$]', '', regex=True, inplace = True)
        file[file.columns[5]].replace('[$]', '', regex=True, inplace = True)
        file[file.columns[6]].replace('[$\\+]', '', regex=True, inplace = True)
        file[file.columns[7]].replace('[%\\+]', '', regex=True, inplace = True)
        file[file.columns[8]].replace('[$]', '', regex=True, inplace = True)
        file[file.columns[9]].replace('[$]', '', regex=True, inplace = True)

        master = master.append(file, ignore_index = True)

    return master

# Parse the etrade files and add them to the master
def parse_etrade(master):
    # Find all of the files in the 'Etrade' folder
    files = [join(pathE, f) for f in listdir(pathE) if isfile(join(pathE, f))]

    # append all of the files to masters
    for f in files:
        fileTOP = pd.read_csv(f, nrows = 1, skiprows = [0])
        fileBOT = pd.read_csv(f, skiprows = 6, skipfooter=6)

        # Get the account name from the top part of the csv
        name = fileTOP.at[0, fileTOP.columns[0]]
        del fileTOP

        # Change the format to be the same as the master
        temp = pd.DataFrame(data={
        master.columns[0] : [name for i in range(fileBOT.shape[0])], # name
        master.columns[1] : fileBOT[fileBOT.columns[0]], # Symbol
        master.columns[3] : fileBOT[fileBOT.columns[6]], # Quantity
        master.columns[4] : fileBOT[fileBOT.columns[1]], # Last Price
        master.columns[5] : fileBOT[fileBOT.columns[7]], # Current Value
        master.columns[6] : fileBOT[fileBOT.columns[4]], # Total Gain/Loss Dollar
        master.columns[7] : fileBOT[fileBOT.columns[5]], # Total Gain/Loss Percent
        master.columns[8] : fileBOT[fileBOT.columns[8]], # Cost Basis Per Share
        # Calculate Cost Basis Total
        master.columns[9] : [p*q for p,q in zip(fileBOT[fileBOT.columns[8]], fileBOT[fileBOT.columns[6]])]
        })

        # append to master
        master = master.append(temp, ignore_index = True)

    return master

# Parse the Sprot files and add them to the master
def parse_sprot(master):
    # Find all of the files in the 'Etrade' folder
    files = [join(pathS, f) for f in listdir(pathS) if isfile(join(pathS, f))]

    # append all of the files to masters
    for f in files:
        fileTOP = pd.read_excel(f, nrows = 1, usecols=np.arange(0,1))
        fileBOT = pd.read_excel(f, skiprows=2, usecols=np.arange(1, 10))

        # Get the name of the account
        name = fileTOP.at[0, fileTOP.columns[0]][9:26]
        del fileTOP

        # Change the format to be the same as the master
        temp = pd.DataFrame(data={
        master.columns[0] : [name for i in range(fileBOT.shape[0])], # Name
        master.columns[1] : fileBOT[fileBOT.columns[1]], # Symbol
        master.columns[2] : fileBOT[fileBOT.columns[0]], # Description
        master.columns[3] : fileBOT[fileBOT.columns[2]], # Quantity
        master.columns[4] : fileBOT[fileBOT.columns[3]], # Last Price
        master.columns[5] : fileBOT[fileBOT.columns[4]], # Current Value
        master.columns[8] : fileBOT[fileBOT.columns[6]], # Cost Basis Per Share
        master.columns[9] : fileBOT[fileBOT.columns[7]] # Cost Basis Total
        })

        # append to master
        master = master.append(temp, ignore_index = True)
    return master

# Parse the Ameritrade files and add them to the master
def parse_ameritrade(master):
    # Find all of the files in the 'Ameritrade' folder
    files = [join(pathA, f) for f in listdir(pathA) if isfile(join(pathA, f))]

    # append all of the files to masters
    for f in files:
        file = pd.read_excel(f, skipfooter=1)

        # Get the data into master format
        temp = pd.DataFrame(data = {
        master.columns[0] : [f[len(f)-18:len(f)-5] for i in range(file.shape[0])], # name
        master.columns[1] : file[file.columns[0]].str.findall("\\(([^\)]+)\\)").str[0], # Symbol
        master.columns[2] : file[file.columns[0]], # Description
        master.columns[3] : file[file.columns[1]], # Quantity
        master.columns[4] : file[file.columns[6]], # Last Price
        master.columns[5] : file[file.columns[7]], # Current Value
        master.columns[6] : file[file.columns[8]], # Total Gain/Loss Dollar
        master.columns[7] : file[file.columns[9]], # Total Gain/Loss Percent
        master.columns[8] : file[file.columns[3]], # Cost Basis Per Share
        master.columns[9] : file[file.columns[4]] # Cost Basis Total
        })

        # Add to master
        master = master.append(temp, ignore_index=True)

    return master

if (__name__ == "__main__"):
    main()
