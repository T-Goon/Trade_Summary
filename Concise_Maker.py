import numpy as np
import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import date
import re
from sys import argv

def main():
    # create concise summary file
    concise = pd.DataFrame(columns = [
    "Symbol", # Symbol
    "Quantity", # Quantity
    "Last Price", # Last Price
    "Current Value", # Current Value
    "Total Gain/Loss Dollar", # Total Gain/Loss Dollar
    "Total Gain/Loss Percent", # Total Gain/Loss Percent
    "Cost Basis Average",
    "Cost Basis Total", # Cost Basis Total
    "Position Size(%)"
    ])

    # Find all of the files in current dir
    files = [f for f in listdir(".") if isfile(f)]
    # Filter out all files in current dir so there is only the summary master left
    files = [f for f in files if "Summary_Master_" in f]

    master = pd.read_csv(files[0], usecols=np.arange(1, 11))

    concise = create_concise(concise, master)

    today = date.today();

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
        if(quan != 0):
            cba = cbt/quan
        else:
            cba = "n/a"

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


if __name__ == "__main__":
    main();
