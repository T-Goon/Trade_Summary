from enum import Enum
import os
import traceback
from typing import List, Optional, Sequence, Tuple, Union
import numpy as np
import pandas as pd
from pandas import DataFrame
import io
from os import listdir
from os.path import isfile, join
from datetime import date

pathFidelity: str = 'Fidelity/'
pathEtrade: str = 'Etrade/'
pathSprott: str = 'sprott/'
pathAmeritrade: str = 'Ameritrade/'
pathCanaccord: str = 'Canaccord/'
pathSchwab: str = 'Schwab/'


class MasterColums(Enum):
    ACCOUNT_NAME: str = 'Account Name/Number'
    SYMBOL: str = 'Symbol'
    DESCRIPTION: str = 'Description'
    QUANTITY: str = 'Quantity'
    LAST_PRICE: str = 'Last Price'
    CURRENT_VALUE: str = 'Current Value'
    TOTAL_GAIN_LOSS_DOLLAR: str = 'Total Gain/Loss Dollar'
    TOTAL_GAIN_LOSS_PERCENT: str = 'Total Gain/Loss Percent'
    COST_BASIS_PER_SHARE: str = 'Cost Basis Per Share'
    TOTAL_COST_BASIS: str = 'Total Cost Basis'


def main():

    # Create master DataFrame from columns from Fidelity files
    master: DataFrame = DataFrame(columns=[
        MasterColums.ACCOUNT_NAME.value,
        MasterColums.SYMBOL.value,
        MasterColums.DESCRIPTION.value,
        MasterColums.QUANTITY.value,
        MasterColums.LAST_PRICE.value,
        MasterColums.CURRENT_VALUE.value,
        MasterColums.TOTAL_GAIN_LOSS_DOLLAR.value,
        MasterColums.TOTAL_GAIN_LOSS_PERCENT.value,
        MasterColums.COST_BASIS_PER_SHARE.value,
        MasterColums.TOTAL_COST_BASIS.value
    ])

    parsers_list = [
        parse_fidelity,
        parse_etrade,
        # parse_sprott,
        # parse_ameritrade,
        parse_canaccord,
        parse_schwab
    ]
    # Parse the files and add to master
    for parser in parsers_list:
        master = parser(master)

    # Get rid of rows with no quantity
    master[MasterColums.QUANTITY.value].replace('', np.nan, inplace=True)
    master.dropna(subset=[MasterColums.QUANTITY.value], inplace=True)

    # Add names of banks at the end
    banks = DataFrame(data={
        MasterColums.ACCOUNT_NAME.value: [  # Account Name
            "QCU", "Etrade", "VioBank", 'Dealmaker', 'Dealmaker', 'Dealmaker',
            'Dealmaker', 'Robin Hood', 'Robin Hood', 'Kraken'
        ],
        MasterColums.SYMBOL.value: [  # Symbol
            "Cash", "Cash", "Cash", 'DTRC', 'Carbon Streaming', 'Carbon Streaming',
            'DTRC', 'BTC', 'Cash', 'BTC'
        ],
        MasterColums.DESCRIPTION.value: [  # Description
            'Cash', 'Cash', 'Cash', 'JR Resources', 'Carbon Streaming', 'Carbon Streaming',
            'Dakota Territory Resource Corp', 'BTC', 'Cash', 'BTC'
        ],
        MasterColums.LAST_PRICE.value:  # Last Price
        [
            '1', '1', '1'
        ] + ([''] * (10-3))
    })
    master = master.append(banks, ignore_index=True)

    # Export file as csv
    today = date.today()
    master.replace('[,]', '', regex=True, inplace=True)
    master.to_csv("Summary_Master_" +
                  today.strftime("%b-%d-%Y") + ".csv", index=False)


def parse_fidelity(master: DataFrame) -> DataFrame:
    """Parse the fidelity files and add them to the master"""
    # Find all of the files in the 'Fidelity' folder
    files = [join(pathFidelity, f) for f in listdir(pathFidelity) if isfile(join(pathFidelity, f))]

    use_cols: int = 15

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()
            data_end: int = -1

            # Find the row where the header ends and the data begins
            rows: List[str] = data.split('\n')
            for i, row in enumerate(rows):
                columns: List[str] = row.split(',')
                if data_end == -1 and columns[0] == '':
                    data_end = len(rows) - i - 1

        _, file = read_file(
            data,
            data_start=None,
            data_end=data_end,
            use_cols=use_cols,
            header=0,
            na_values=['--', 'n/a']
        )

        # drop useless columns
        file = file.drop(
            columns=[
                "Last Price Change",
                "Today's Gain/Loss Dollar", 
                "Today's Gain/Loss Percent", 
                "Type",
                "Percent Of Account"
            ],
            errors='ignore'
        )

        # Get rid of special characters
        file.replace('[%]', '', regex=True, inplace=True)
        remove_bad_characters(file, 4)
        remove_bad_characters(file, 5)
        remove_bad_characters(file, 6)
        remove_bad_characters(file, 7)
        remove_bad_characters(file, 8)
        remove_bad_characters(file, 9)
        remove_bad_characters(file, 10)

        # Condense Account name and number columns into one
        if (MasterColums.ACCOUNT_NAME.value not in file.columns):
            file[MasterColums.ACCOUNT_NAME.value] = file[file.columns[0]] + \
                " " + file[file.columns[1]]
            file = file.drop(columns=['Account Number', 'Account Name'])

        # Handle renamed Total Cost Basis column
        if (MasterColums.TOTAL_COST_BASIS.value not in file.columns and 'Cost Basis Total' in file.columns):
            file = file.rename(
                columns={'Cost Basis Total': MasterColums.TOTAL_COST_BASIS.value})

        # Handle renamed Cost Basis Per Share column
        if (MasterColums.COST_BASIS_PER_SHARE.value not in file.columns and 'Average Cost Basis' in file.columns):
            file = file.rename(
                columns={'Average Cost Basis': MasterColums.COST_BASIS_PER_SHARE.value})

        file[MasterColums.DESCRIPTION.value] = file[MasterColums.DESCRIPTION.value].fillna('').astype(str)
        # Money Market and Pending Activity has no quantity, set to 1
        for i in range(0, file.shape[0]):
            description: str = file.loc[i, MasterColums.DESCRIPTION.value].lower()
            
            if ('money market' in description or 
                'pending activity' in description):
                file.loc[i, MasterColums.QUANTITY.value] = file.loc[i, MasterColums.CURRENT_VALUE.value]
                file.loc[i, MasterColums.LAST_PRICE.value] = 1

        # Total gain/loss dollar always positive, use percentage to check if it should be negative
        # file['Total Gain/Loss Percent'] = pd.to_numeric(
        #     file['Total Gain/Loss Percent'])
        # file['Total Gain/Loss Dollar'] = [  
        #     ('-' + str(dollar)) if percent < 0 else dollar
        #     for dollar, percent in zip(file['Total Gain/Loss Dollar'], file['Total Gain/Loss Percent'])]

        master = master.append(file, ignore_index=True)

    return master


def parse_etrade(master: DataFrame) -> DataFrame:
    """Parse the etrade files and add them to the master"""
    # Find all of the files in the 'Etrade' folder
    files = [join(pathEtrade, f) for f in listdir(pathEtrade) if isfile(join(pathEtrade, f))]

    use_cols: int = 12

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data: str = fr.read()
            data_start: int = -1
            data_end: int = -1
            # Etrade places a bad row in the table that says you have
            # no positions if you don't have any positions.
            # Must be removed for parsing.
            bad_rows: List[int] = []

            # Find the row where the header ends and the data begins
            rows: List[str] = data.split('\n')
            for i, row in enumerate(rows):
                columns: List[str] = row.split(',')
                if data_start != -1 and i < len(rows) -  data_end and len(columns) < 12:
                    bad_rows.append(i)
                if data_start == -1 and columns[0] == 'Symbol' and columns[1] == 'Qty #':
                    data_start = i
                if data_end == -1 and columns[0] == 'TOTAL':
                    data_end = len(rows) - i - 1

        fileTOP, fileBOT = read_file(
            file=data,
            data_start=data_start,
            data_end=data_end,
            use_cols=use_cols,
            skip_rows_top=[0],
            na_values=[''],
            header=0,
            skip_rows_bot=bad_rows
        )

        # Get the account name from the top part of the csv
        name = fileTOP.at[0, fileTOP.columns[0]]
        del fileTOP

        # Change the format to be the same as the master
        temp: DataFrame = DataFrame(data={
            # name
            MasterColums.ACCOUNT_NAME.value: [name for _ in range(fileBOT.shape[0])],
            # Symbol
            MasterColums.SYMBOL.value: fileBOT[fileBOT.columns[0]],  
            # Quantity
            MasterColums.QUANTITY.value: fileBOT[fileBOT.columns[1]],
            # Last Price
            MasterColums.LAST_PRICE.value: fileBOT[fileBOT.columns[2]],
            # Current Value
            MasterColums.CURRENT_VALUE.value: fileBOT[fileBOT.columns[3]],
            # Total Gain/Loss Dollar
            MasterColums.TOTAL_GAIN_LOSS_DOLLAR.value: fileBOT[fileBOT.columns[5]],
            # Total Gain/Loss Percent
            MasterColums.TOTAL_GAIN_LOSS_PERCENT.value: fileBOT[fileBOT.columns[7]],
            # Cost Basis Per Share
            MasterColums.COST_BASIS_PER_SHARE.value: fileBOT[fileBOT.columns[9]],
            # Cost Basis Total
            MasterColums.TOTAL_COST_BASIS.value: fileBOT[fileBOT.columns[11]]
        })       

        # Cash has no quantity or last price for some reason. Set it.
        for i in range(0, temp.shape[0]):
            symbol: str = temp.loc[i, MasterColums.SYMBOL.value].lower()
            
            if (symbol == 'cash'):
                temp.loc[i, MasterColums.QUANTITY.value] = temp.loc[i, MasterColums.CURRENT_VALUE.value]
                temp.loc[i, MasterColums.LAST_PRICE.value] = 1

        # append to master
        master = master.append(temp, ignore_index=True)

    return master


@DeprecationWarning
def parse_sprott(master: DataFrame) -> DataFrame:
    """Parse the sprott files and add them to the master"""
    # Find all of the files in the 'Sprott' folder
    files = [join(pathSprott, f) for f in listdir(pathSprott) if isfile(join(pathSprott, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data: str = fr.read()
            data_start: int = -1

            # Find the row where the header ends and the data begins
            for i, row in enumerate(data.split('\n')):
                columns: List[str] = row.split(',')
                if columns[0] == 'Description':
                    data_start = i
                    break

        try:
            fileTOP = pd.read_excel(
                io.StringIO(data),
                nrows=1,
                usecols=np.arange(0, 1)
            )
            fileBOT = pd.read_excel(
                io.StringIO(
                    data),
                skiprows=data_start,
                usecols=np.arange(0, 10)
            )
        except:
            fileTOP = pd.read_csv(
                io.StringIO(data),
                nrows=1,
                usecols=np.arange(0, 1),
                engine='python',
                error_bad_lines=False
            )
            fileBOT = pd.read_csv(
                io.StringIO(data),
                skiprows=data_start,
                usecols=np.arange(0, 10),
                engine='python',
                error_bad_lines=False
            )

        # Get the name of the account
        name = fileTOP.at[0, fileTOP.columns[0]][9:26]
        del fileTOP

        # Change the format to be the same as the master
        temp = DataFrame(data={
            # Name
            MasterColums.ACCOUNT_NAME.value: [name for _ in range(fileBOT.shape[0])],
            MasterColums.SYMBOL.value: fileBOT[fileBOT.columns[1]],  # Symbol
            # Description
            MasterColums.DESCRIPTION.value: fileBOT[fileBOT.columns[0]],
            # Quantity
            MasterColums.QUANTITY.value: fileBOT[fileBOT.columns[2]],
            # Last Price
            MasterColums.LAST_PRICE.value: fileBOT[fileBOT.columns[3]],
            # Current Value
            MasterColums.CURRENT_VALUE.value: fileBOT[fileBOT.columns[4]],
            # Cost Basis Per Share
            MasterColums.COST_BASIS_PER_SHARE.value: fileBOT[fileBOT.columns[8]],
            # Cost Basis Total
            MasterColums.TOTAL_COST_BASIS.value: fileBOT[fileBOT.columns[9]]
        })

        temp[MasterColums.CURRENT_VALUE.value].replace(
            '[\\*]', '', regex=True, inplace=True)

        # append to master
        master = master.append(temp, ignore_index=True)
    return master


@DeprecationWarning
def parse_ameritrade(master: DataFrame) -> DataFrame:
    """Parse the Ameritrade files and add them to the master"""
    # Find all of the files in the 'Ameritrade' folder
    files = [join(pathAmeritrade, f) for f in listdir(pathAmeritrade) if isfile(join(pathAmeritrade, f))]

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data = fr.read()

        try:
            file = pd.read_excel(io.StringIO(data), skipfooter=1)
        except:
            file = pd.read_csv(io.StringIO(data), skipfooter=1,
                               engine='python', error_bad_lines=False)

        # Get the data into master format
        temp = DataFrame(data={
            # name
            MasterColums.ACCOUNT_NAME.value: [f[len(f)-18:len(f)-5] for i in range(file.shape[0])],
            # Symbol
            MasterColums.SYMBOL.value: file[file.columns[0]].str.findall("\\(([^\)]+)\\)").str[0],
            # Description
            MasterColums.DESCRIPTION.value: file[file.columns[0]],
            MasterColums.QUANTITY.value: file[file.columns[1]],  # Quantity
            MasterColums.LAST_PRICE.value: file[file.columns[6]],  # Last Price
            # Current Value
            MasterColums.CURRENT_VALUE.value: file[file.columns[7]],
            # Total Gain/Loss Dollar
            MasterColums.TOTAL_GAIN_LOSS_DOLLAR.value: file[file.columns[8]],
            # Total Gain/Loss Percent
            MasterColums.TOTAL_GAIN_LOSS_PERCENT.value: file[file.columns[9]],
            # Cost Basis Per Share
            MasterColums.COST_BASIS_PER_SHARE.value: file[file.columns[3]],
            # Cost Basis Total
            MasterColums.TOTAL_COST_BASIS.value: file[file.columns[4]]
        })

        # Add to master
        master = master.append(temp, ignore_index=True)

    return master


def parse_canaccord(master: DataFrame) -> DataFrame:
    """Parse the Canaccord files and add them to the master"""
    # Find all of the files in the 'Canaccord' folder
    files = [join(pathCanaccord, f) for f in listdir(pathCanaccord) if isfile(join(pathCanaccord, f))]

    # append all of the files to masters
    for f in files:
        # with open(f, encoding='ascii', errors='ignore') as fr:
        #     data = fr.read()

        _, file = read_file(
            f,
            data_start=2
        )

        # Get the data into master format
        temp: DataFrame = DataFrame(data={
            # name
            MasterColums.ACCOUNT_NAME.value: file[file.columns[2]],
            # Symbol
            MasterColums.SYMBOL.value: file[file.columns[0]],
            # Description
            MasterColums.DESCRIPTION.value: file[file.columns[4]],
            MasterColums.QUANTITY.value: file[file.columns[6]],  # Quantity
            MasterColums.LAST_PRICE.value: file[file.columns[7]],  # Last Price
            # Current Value
            MasterColums.CURRENT_VALUE.value: file[file.columns[12]],
            # Total Gain/Loss Dollar
            MasterColums.TOTAL_GAIN_LOSS_DOLLAR.value: [''] * file.shape[0],
            # Total Gain/Loss Percent
            MasterColums.TOTAL_GAIN_LOSS_PERCENT.value: [''] * file.shape[0],
            # Cost Basis Per Share
            MasterColums.COST_BASIS_PER_SHARE.value: [''] * file.shape[0],
            MasterColums.TOTAL_COST_BASIS.value: [
                ''] * file.shape[0]  # Cost Basis Total
        })

        # Add to master
        master = master.append(temp, ignore_index=True)

    return master


def parse_schwab(master: DataFrame) -> DataFrame:
    """Parse the schwab files and add them to the master"""
    # Find all of the files in the 'Schwab' folder
    files = [join(pathSchwab, f)
             for f in listdir(pathSchwab) if isfile(join(pathSchwab, f))]

    use_cols: int = 12

    # append all of the files to masters
    for f in files:
        with open(f, encoding='ascii', errors='ignore') as fr:
            data: str = fr.read()
            data_start: int = -1
            data_end: int = -1

            # Find the row where the header ends and the data begins
            rows: List[str] = data.split('\n')
            for i, row in enumerate(rows):
                columns: List[str] = row.split(',')
                if data_start == -1 and 'Symbol' in columns[0] and 'Description' in columns[1]:
                    data_start = i
                if data_end == -1 and 'Account Total' in columns[0]:
                    data_end = len(rows) - i - 1

        fileTOP, fileBOT = read_file(
            file=data,
            data_start=data_start,
            data_end=data_end,
            use_cols=use_cols,
            na_values='--'
        )

        # Get the account name from the top part of the csv
        name: str = fileTOP.columns[0]
        dot_index: int = name.find('...')
        name = 'Schwab - ' + name[dot_index:dot_index+6]
        del fileTOP

        # Get rid of bad characters and convert strings to floats
        remove_bad_characters(fileBOT, 3)
        remove_bad_characters(fileBOT, 6)
        remove_bad_characters(fileBOT, 10)
        fileBOT[fileBOT.columns[10]] = pd.to_numeric(
            fileBOT[fileBOT.columns[10]])
        remove_bad_characters(fileBOT, 11)
        fileBOT[fileBOT.columns[11]] = pd.to_numeric(
            fileBOT[fileBOT.columns[11]])
        remove_bad_characters(fileBOT, 2)
        fileBOT[fileBOT.columns[2]] = pd.to_numeric(
            fileBOT[fileBOT.columns[2]])
        remove_bad_characters(fileBOT, 9)
        fileBOT[fileBOT.columns[9]] = pd.to_numeric(
            fileBOT[fileBOT.columns[9]])

        # Change the format to be the same as the master
        temp = DataFrame(data={
            # name
            MasterColums.ACCOUNT_NAME.value: [name for _ in range(fileBOT.shape[0])],
            MasterColums.SYMBOL.value: fileBOT[fileBOT.columns[0]],  # Symbol
            # Quantity
            MasterColums.QUANTITY.value: fileBOT[fileBOT.columns[2]],
            # Last Price
            MasterColums.LAST_PRICE.value: fileBOT[fileBOT.columns[3]],
            # Current Value
            MasterColums.CURRENT_VALUE.value: fileBOT[fileBOT.columns[6]],
            # Total Gain/Loss Dollar
            MasterColums.TOTAL_GAIN_LOSS_DOLLAR.value: [  # Always positive, use percentage to check if it should be negative
                -1 * dollar if percent < 0 else dollar
                for dollar, percent in zip(fileBOT[fileBOT.columns[11]], fileBOT[fileBOT.columns[10]])],
            # Total Gain/Loss Percent
            MasterColums.TOTAL_GAIN_LOSS_PERCENT.value: fileBOT[fileBOT.columns[10]],
            # Calculate Cost Basis Per Share
            MasterColums.COST_BASIS_PER_SHARE.value: [
                p/q
                for p, q in zip(fileBOT[fileBOT.columns[9]], fileBOT[fileBOT.columns[2]])],
            # Cost Basis Total
            MasterColums.TOTAL_COST_BASIS.value: fileBOT[fileBOT.columns[9]],
        })
        
        # Cash has no quantity or last price for some reason. Set it.
        for i in range(0, temp.shape[0]):
            symbol: str = temp.loc[i, MasterColums.SYMBOL.value].lower()
            
            if ('cash' in symbol):
                temp.loc[i, MasterColums.QUANTITY.value] = temp.loc[i, MasterColums.CURRENT_VALUE.value]
                temp.loc[i, MasterColums.LAST_PRICE.value] = 1

        # append to master
        master = master.append(temp, ignore_index=True)

    return master


def read_file(
        file: str, 
        data_start: Optional[int],
        use_cols: int=None, 
        data_end: int=0, 
        skip_rows_top=None,
        header: Union[int, Sequence[int], str, None] ='infer',
        na_values: Optional[Union[str, List[str]]]=None,
        skip_rows_bot: List[int] = []
    ) -> Tuple[DataFrame, DataFrame]:
    """Read a Excel or CSV file and convert them to Pandas dataframes.

    Args:
        file (str): Excel file path or CSV file contents
        data_start (int): Number of rows to skip before the data is actually shown
        use_cols (int, optional): Number of columns to read. Defaults to None.
        data_end (int, optional): Number of columns to skip at the end of the sheet. Defaults to 0.
        skip_rows_top (int, optional): Number of rows to skip to get the correct data for the first dataframe. Defaults to None.
        header (Union[int, Sequence[int], Literal["infer"], None], optional): Header names to use. Defaults to 'infer'.
        na_values (Optional[Union[str, List[str]]], optional): The null values in the shpreadsheet. Defaults to None.

    Returns:
        Tuple[DataFrame, DataFrame]: 2 dataframes. The first is a single row dataframe used for extracting the
        account names if needed. The second contains the data.
    """
    fileTOP: Union[DataFrame, None] = None
    fileBOT: Union[DataFrame, None] = None
    
    try:
        fileTOP = pd.read_excel(
            file,
            nrows=1,
            skiprows=skip_rows_top
        )
        fileBOT = pd.read_excel(
            file,
            header=header,
            skiprows=list(range(data_start)) + skip_rows_bot if data_start != None else skip_rows_bot,
            skipfooter=data_end,
            usecols=range(use_cols) if use_cols is not None else None,
            na_values=na_values
        )
    except Exception as e:
        fileTOP = pd.read_csv(
            io.StringIO(file),
            nrows=1,
            skiprows=skip_rows_top,
            engine='python',
            on_bad_lines='warn'
        )
        fileBOT = pd.read_csv(
            io.StringIO(file),
            header=header,
            skiprows=list(range(data_start)) + skip_rows_bot if data_start != None else skip_rows_bot,
            skipfooter=data_end,
            usecols=range(use_cols) if use_cols is not None else None,
            engine='python',
            on_bad_lines='warn',
            na_values=na_values,
            index_col=False
        )

    return fileTOP, fileBOT


def remove_bad_characters(dataframe: DataFrame, index: int):
    """Removes currency formatting from number strings for a dataframe column.

    Args:
        dataframe (DataFrame): Dataframe to format the column for.
        index (int): Index of the column to format.
    """
    dataframe[dataframe.columns[index]].replace(
        '[%\\+\\(\\)$,]',
        '', 
        regex=True, 
        inplace=True
    )


if (__name__ == "__main__"):
    try:
        if os.path.exists('error.log'):
            os.remove("error.log")

        main()
    except Exception as e:
        with open('error.log', 'w', encoding='utf-8') as error_file:
            error_file.write(str(e))
            traceback.print_exc()
