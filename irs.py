import pandas as pd
import pandas_read_xml as pdx
import numpy as np
import argparse
import multiprocessing
import glob
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument(help="target directory(s)", default=".", nargs="+", dest="path")
parser.add_argument("-o", help="output file", default="output.txt", dest="o")
parser.add_argument(
    "-t", help="number of threads to use", default=1, dest="threads", type=int
)

args = parser.parse_args()

paths = []
for path in args.path:
    paths.append(os.path.expanduser(path))

## Prepare multithreading.

try:
    multiprocessing.set_start_method("fork")  # default for >=3.8 is spawn
except RuntimeError:  # there might be a timeout sometimes.
    pass

m = multiprocessing.Manager()
lock = m.Lock()


def get_columns_990PF(path):
    #df = pdx.read_xml(path, ["Return"]) # file already read in
    data = pdx.fully_flatten(path)
    ## if columns don't exist, return them with an empty entry. If there is an easier way to do this, please let me know!
    if "ReturnHeader|Filer|EIN" not in data.columns:
        data["ReturnHeader|Filer|EIN"] = None
    if "ReturnHeader|Filer|BusinessNameControlTxt" not in data.columns:
        data["ReturnHeader|Filer|BusinessNameControlTxt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonNm" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonNm"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonTitleTxt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PhoneNum" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PhoneNum"] = None
    if "ReturnHeader|BusinessOfficerGrp|SignatureDt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|SignatureDt"] = None
    if "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|AddressLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|USAddress|AddressLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|CityNm" not in data.columns:
        data["ReturnHeader|Filer|USAddress|CityNm"] = None
    if "ReturnHeader|Filer|USAddress|StateAbbreviationCd" not in data.columns:
        data["ReturnHeader|Filer|USAddress|StateAbbreviationCd"] = None
    if "ReturnHeader|Filer|USAddress|ZIPCd" not in data.columns:
        data["ReturnHeader|Filer|USAddress|ZIPCd"] = None
    if "ReturnHeader|TaxYr" not in data.columns:
        data["ReturnHeader|TaxYr"] = None
    if "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesRevAndExpnssAmt" not in data.columns:
        data["ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesRevAndExpnssAmt"] = None
    if "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesNetInvstIncmAmt" not in data.columns:
        data["ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesNetInvstIncmAmt"] = None
    if "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesAdjNetIncmAmt" not in data.columns:
        data["ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesAdjNetIncmAmt"] = None
    if "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesDsbrsChrtblAmt" not in data.columns:
        data["ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesDsbrsChrtblAmt"] = None
    if "ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsBOYAmt" not in data.columns:
        data["ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsBOYAmt"] = None
    if "ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsEOYAmt" not in data.columns:
        data["ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsEOYAmt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|TitleTxt" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|TitleTxt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|AverageHrsPerWkDevotedToPosRt" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|AverageHrsPerWkDevotedToPosRt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|CompensationAmt" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|CompensationAmt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|BusinessName|BusinessNameLine1Txt" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|BusinessName|BusinessNameLine1Txt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|AddressLine1Txt" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|AddressLine1Txt"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|CityNm" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|CityNm"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|StateAbbreviationCd" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|StateAbbreviationCd"] = None
    if "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|ZIPCd" not in data.columns:
        data["ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|ZIPCd"] = None

    extracted_data = data[
        [
            "ReturnHeader|Filer|EIN",
            "ReturnHeader|Filer|BusinessNameControlTxt",
            "ReturnHeader|BusinessOfficerGrp|PersonNm",
            "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt",
            "ReturnHeader|BusinessOfficerGrp|PhoneNum",
            "ReturnHeader|BusinessOfficerGrp|SignatureDt",
            "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt",
            "ReturnHeader|Filer|USAddress|AddressLine1Txt",
            "ReturnHeader|Filer|USAddress|CityNm",
            "ReturnHeader|Filer|USAddress|StateAbbreviationCd",
            "ReturnHeader|Filer|USAddress|ZIPCd",
            "ReturnHeader|TaxYr",
            "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesRevAndExpnssAmt",
            "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesNetInvstIncmAmt",
            "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesAdjNetIncmAmt",
            "ReturnData|IRS990PF|AnalysisOfRevenueAndExpenses|TotalExpensesDsbrsChrtblAmt",
            "ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsBOYAmt",
            "ReturnData|IRS990PF|Form990PFBalanceSheetsGrp|TotalAssetsEOYAmt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|TitleTxt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|AverageHrsPerWkDevotedToPosRt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|CompensationAmt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|BusinessName|BusinessNameLine1Txt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|AddressLine1Txt",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|CityNm",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|StateAbbreviationCd",
            "ReturnData|IRS990PF|OfficerDirTrstKeyEmplInfoGrp|OfficerDirTrstKeyEmplGrp|USAddress|ZIPCd",
        ]
    ].drop_duplicates()
    return extracted_data


def get_columns_990(df):
    #df = pdx.read_xml(path, ["Return"])
    data = pdx.fully_flatten(df)
    
    ## a whole load of if statements
    if "ReturnHeader|Filer|EIN" not in data.columns:
        data["ReturnHeader|Filer|EIN"] = None
    if "ReturnHeader|Filer|BusinessNameControlTxt" not in data.columns:
        data["ReturnHeader|Filer|BusinessNameControlTxt"] = None
    if "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|AddressLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|USAddress|AddressLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|CityNm" not in data.columns:
        data["ReturnHeader|Filer|USAddress|CityNm"] = None
    if "ReturnHeader|Filer|USAddress|StateAbbreviationCd" not in data.columns:
        data["ReturnHeader|Filer|USAddress|StateAbbreviationCd"] = None
    if data["ReturnHeader|Filer|USAddress|ZIPCd"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|ZIPCd"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonNm" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonNm"] = None
    if "ReturnHeader|BusinessOfficerGrp|SignatureDt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|SignatureDt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonTitleTxt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PhoneNum" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PhoneNum"] = None
    if "ReturnHeader|TaxYr" not in data.columns:
        data["ReturnHeader|TaxYr"] = None
    if "ReturnData|IRS990|OfficeExpensesGrp|TotalAmt" not in data.columns:
        data["ReturnData|IRS990|OfficeExpensesGrp|TotalAmt"] = None
    if "ReturnData|IRS990|TotalFunctionalExpensesGrp|TotalAmt" not in data.columns:
        data["ReturnData|IRS990|TotalFunctionalExpensesGrp|TotalAmt"] = None
    if "ReturnData|IRS990|OtherExpensesGrp|TotalAmt" not in data.columns:
        data["ReturnData|IRS990|OtherExpensesGrp|TotalAmt"] = None
    if "ReturnData|IRS990|TotalAssetsBOYAmt" not in data.columns:
        data["ReturnData|IRS990|TotalAssetsBOYAmt"] = None
    if "ReturnData|IRS990|TotalAssetsEOYAmt" not in data.columns:
        data["ReturnData|IRS990|TotalAssetsEOYAmt"] = None
    if "ReturnData|IRS990|OfficerMailingAddressInd" not in data.columns:
        data["ReturnData|IRS990|OfficerMailingAddressInd"] = None
    if "ReturnData|IRS990|Form990PartVIISectionAGrp|PersonNm" not in data.columns:
        data["ReturnData|IRS990|Form990PartVIISectionAGrp|PersonNm"] = None
    if "ReturnData|IRS990|Form990PartVIISectionAGrp|TitleTxt" not in data.columns:
        data["ReturnData|IRS990|Form990PartVIISectionAGrp|TitleTxt"] = None
    
    
    
    
    extracted_data = data[
        [
            "ReturnHeader|Filer|EIN",
            "ReturnHeader|Filer|BusinessNameControlTxt",
            "ReturnHeader|BusinessOfficerGrp|PersonNm",
            "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt",
            "ReturnHeader|BusinessOfficerGrp|PhoneNum",
            "ReturnHeader|BusinessOfficerGrp|SignatureDt",
            "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt",
            "ReturnHeader|Filer|USAddress|AddressLine1Txt",
            "ReturnHeader|Filer|USAddress|CityNm",
            "ReturnHeader|Filer|USAddress|StateAbbreviationCd",
            "ReturnHeader|Filer|USAddress|ZIPCd",
            "ReturnHeader|TaxYr",
            "ReturnData|IRS990|OfficeExpensesGrp|TotalAmt",
            "ReturnData|IRS990|TotalFunctionalExpensesGrp|TotalAmt",
            "ReturnData|IRS990|OtherExpensesGrp|TotalAmt",
            "ReturnData|IRS990|TotalAssetsBOYAmt",
            "ReturnData|IRS990|TotalAssetsEOYAmt",
            "ReturnData|IRS990|PrincipalOfficerNm",
            "ReturnData|IRS990|OfficerMailingAddressInd",
            "ReturnData|IRS990|Form990PartVIISectionAGrp|PersonNm",
            "ReturnData|IRS990|Form990PartVIISectionAGrp|TitleTxt",
        ]
    ].drop_duplicates()
    
    return extracted_data


# extracted_data.to_csv(args.o,mode="a") issue with this one is that it will export the column name every time

# extracted_data_str = " ".join(([str(i) for i in extracted_data]))
# with open(args.o,"a") as output:
#    output.write(extracted_data_str + "\n")


def get_columns_990ez(data):
    #df = pdx.read_xml(path, ["Return"])
    data = pdx.fully_flatten(data)
    
    ## last bunch of a lot of if statements
    
    if "ReturnHeader|Filer|EIN" not in data.columns:
        data["ReturnHeader|Filer|EIN"] = None
    if "ReturnHeader|Filer|BusinessNameControlTxt" not in data.columns:
        data["ReturnHeader|Filer|BusinessNameControlTxt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonNm" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonNm"] = None
    if "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PersonTitleTxt"] = None
    if "ReturnHeader|BusinessOfficerGrp|PhoneNum" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|PhoneNum"] = None
    if "ReturnHeader|BusinessOfficerGrp|SignatureDt" not in data.columns:
        data["ReturnHeader|BusinessOfficerGrp|SignatureDt"] = None
    if "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|AddressLine1Txt" not in data.columns:
        data["ReturnHeader|Filer|USAddress|AddressLine1Txt"] = None
    if "ReturnHeader|Filer|USAddress|CityNm" not in data.columns:
        data["ReturnHeader|Filer|USAddress|CityNm"] = None
    if "ReturnHeader|Filer|USAddress|StateAbbreviationCd" not in data.columns:
        data["ReturnHeader|Filer|USAddress|StateAbbreviationCd"] = None
    if data["ReturnHeader|Filer|USAddress|ZIPCd"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|ZIPCd"] = None
    if data["ReturnHeader|TaxYr"] not in data.columns:
        data["ReturnHeader|TaxYr"] = None
    if data["ReturnData|IRS990EZ|TotalRevenueAmt"] not in data.columns:
        data["ReturnData|IRS990EZ|TotalRevenueAmt"] = None
    if data["ReturnData|IRS990EZ|TotalExpensesAmt"] not in data.columns:
        data["ReturnData|IRS990EZ|TotalExpensesAmt"] = None
    if data["ReturnData|IRS990EZ|ContributionsGiftsGrantsEtcAmt"] not in data.columns:
        data["ReturnData|IRS990EZ|ContributionsGiftsGrantsEtcAmt"] = None
    if data["ReturnData|IRS990EZ|CashSavingsAndInvestmentsGrp|BOYAmt"] not in data.columns:
        data["ReturnData|IRS990EZ|CashSavingsAndInvestmentsGrp|BOYAmt"] = None
    if data["ReturnData|IRS990EZ|Form990TotalAssetsGrp|EOYAmt"] not in data.columns:
        data["ReturnData|IRS990EZ|Form990TotalAssetsGrp|EOYAmt"] = None
    if data["ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|PersonNm"] not in data.columns:
        data["ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|PersonNm"] = None
    if data["ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|TitleTxt"] not in data.columns:
        data["ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|TitleTxt"] = None
    if data["ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt"] not in data.columns:
        data["ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt"] = None
    if data["ReturnHeader|Filer|USAddress|AddressLine1Txt"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|AddressLine1Txt"] = None
    if data["ReturnHeader|Filer|USAddress|CityNm"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|CityNm"] = None
    if data["ReturnHeader|Filer|USAddress|StateAbbreviationCd"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|StateAbbreviationCd"] = None
    if data["ReturnHeader|Filer|USAddress|ZIPCd"] not in data.columns:
        data["ReturnHeader|Filer|USAddress|ZIPCd"] = None
    
    extracted_data = data[
        [
            "ReturnHeader|Filer|EIN",
            "ReturnHeader|Filer|BusinessNameControlTxt",
            "ReturnHeader|BusinessOfficerGrp|PersonNm",
            "ReturnHeader|BusinessOfficerGrp|PersonTitleTxt",
            "ReturnHeader|BusinessOfficerGrp|PhoneNum",
            "ReturnData|IRS990EZ|BooksInCareOfDetail|BusinessName|BusinessNameLine1Txt",
            "ReturnData|IRS990EZ|BooksInCareOfDetail|USAddress|AddressLine1Txt",
            "ReturnData|IRS990EZ|BooksInCareOfDetail|USAddress|CityNm",
            "ReturnData|IRS990EZ|BooksInCareOfDetail|USAddress|StateAbbreviationCd",
            "ReturnData|IRS990EZ|BooksInCareOfDetail|USAddress|ZIPCd",
            "ReturnHeader|TaxYr",
            "ReturnData|IRS990EZ|TotalRevenueAmt",
            "ReturnData|IRS990EZ|TotalExpensesAmt",
            "ReturnData|IRS990EZ|ContributionsGiftsGrantsEtcAmt",
            "ReturnData|IRS990EZ|CashSavingsAndInvestmentsGrp|BOYAmt",
            "ReturnData|IRS990EZ|CashSavingsAndInvestmentsGrp|EOYAmt",
            "ReturnData|IRS990EZ|Form990TotalAssetsGrp|BOYAmt",
            "ReturnData|IRS990EZ|Form990TotalAssetsGrp|EOYAmt",
            "ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|PersonNm",
            "ReturnData|IRS990EZ|OfficerDirectorTrusteeEmplGrp|TitleTxt",
            "ReturnHeader|Filer|BusinessName|BusinessNameLine1Txt",
            "ReturnHeader|Filer|USAddress|AddressLine1Txt",
            "ReturnHeader|Filer|USAddress|CityNm",
            "ReturnHeader|Filer|USAddress|StateAbbreviationCd",
            "ReturnHeader|Filer|USAddress|ZIPCd",
        ]
    ].drop_duplicates()
    
    return extracted_data


def extract_irs_data(path):
    df = pdx.read_xml(path,['Return'])
    new_df = pdx.flatten(df)
    if new_df["ReturnHeader|ReturnTypeCd"][0] == "990":
        extracted_data = get_columns_990(df)
        print(path, "IRS990")  # process 990 columns
    elif new_df["ReturnHeader|ReturnTypeCd"][0] == "990PF":
        extracted_data = get_columns_990PF(df)
        print(path, "IRS990PF")  # process 990pf columns
    elif new_df["ReturnHeader|ReturnTypeCd"][0] == "990EZ":
        extracted_data = get_columns_990ez(df)
        print(path, "IRS990EZ")  # process 990ez columns
#except Exception as e:
#    print(f"File had an error, which was {e}. skipping...")
        
    #print(extracted_data)


#def save_to_file(data)

if __name__ == "__main__":

    pool = multiprocessing.Pool(processes=args.threads)
    for path in paths:
        if not os.path.isdir(path):
            if os.path.isfile(path):
                data = extract_irs_data(path)
                sys.exit()
 
        files = glob.glob(os.path.join(path, "*"))
        pool.map(extract_irs_data,files)
            #pool.map(extract_irs_data)
