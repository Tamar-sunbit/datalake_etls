import pandas as pd


def normalize_df(df, norm_field_list):
    norm_df = df
    for fld in norm_field_list:
        norm = pd.json_normalize(df[fld])
        dups = [c for c in norm.columns if c in df.columns]
        if len(dups) > 0:
            norm.rename(columns={x: f'{fld}_{x}' for x in dups}, inplace=True)
            # norm_df.drop(columns=dups, inplace=True)
        norm_df = pd.concat([norm_df, norm], axis=1)
    norm_df.drop(columns=norm_field_list, inplace=True)
    return norm_df
