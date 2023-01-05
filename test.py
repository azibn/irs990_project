import pandas as pd
from thefuzz import fuzz
import os
import multiprocessing
import argparse
import glob

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

try:
    multiprocessing.set_start_method("fork")  # default for >=3.8 is spawn
except RuntimeError:  # there might be a timeout sometimes.
    pass

pf_names = pd.read_csv('To_Azib/pf_names_v2.csv')
codes = pd.read_excel('ZIP_COUNTY_122021.xlsx')
codes['zip'] = codes['zip'].astype(str).str.zfill(5)
test = codes[['zip','county','usps_zip_pref_city','usps_zip_pref_state']].loc[codes.sort_values(by=['tot_ratio'],ascending=False).tot_ratio >= 0.90]
test['zip'] = test['zip'].astype(str).str.zfill(5)
pf_names.gm_city = pf_names.gm_city.str.upper()
pf_names.first_name = pf_names.first_name.str.upper()
pf_names.last_name = pf_names.last_name.str.upper()
pf_names.name = pf_names.name.str.upper()
pf_names_updated = pd.merge(left=pf_names,right=test,right_on=['usps_zip_pref_city','usps_zip_pref_state'],left_on=['gm_city','gm_state'])
pf_names_updated.drop(['usps_zip_pref_city','usps_zip_pref_state'],axis=1,inplace=True)


def process(path):
    print(path)
    data = pd.read_csv(path)
    data = data.rename(columns = {'last name':'last_name','first name':'first_name'})
    data.columns = [f"df1_{col}" for col in data.columns]

    # Add new columns
    data["fuzz_ratio_lname"] = (
        data["df1_last_name"]
        .apply(
            lambda x: max(
                [(value, fuzz.ratio(x, value)) for value in pf_names_updated["last_name"]],
                key=lambda x: x[1],
            )
        )
        .apply(lambda x: x if x[1] > 80 else pd.NA)
    )

    data[["df2_lname", "fuzz_ratio_lname"]] = pd.DataFrame(
        data["fuzz_ratio_lname"].tolist(), index=data.index
    )
    df1 = (
        pd.merge(left=data, right=pf_names_updated, how="left", left_on="df2_lname", right_on="last_name")
        .drop(columns="lname")
        .rename(columns={"fname": "df2_fname"})
    )

    data["df2_fname"] = data["df2_fname"].fillna(value="")
    for i, (x, value) in enumerate(zip(data["df1_fname"], data["df2_fname"])):
        ratio = fuzz.ratio(x, value)
        data.loc[i, "fuzz_ratio_fname"] = ratio if ratio > 60 else pd.NA

    # Cleanup
    data["df2_fname"] = data["df2_fname"].replace("", pd.NA)
    data = data[
        [
            "df1_ein",
            "df1_ein_name",
            "df1_lname",
            "df1_fname",
            "fuzz_ratio_lname",
            "fuzz_ratio_fname",
            "df2_lname",
            "df2_fname",
            "score",
        ]
    ]

    try:
        os.makedirs("final_results/")
    except:
        pass
    data.to_csv(f'final_results/matched_data_{data.county.unique()}.csv')
    return data

if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=args.threads)
    for path in paths:
       
        files = glob.glob(os.path.join(path, "*"))
        pool.map(process, files)




