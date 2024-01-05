import sys
import pandas as pd

import numpy as np

#from matplotlib import pyplot as plt

import scipy.stats
#from statsmodels.stats.multicomp import pairwise_tukeyhsd

import glob

def get_observs(path):
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, names = None))
    
    df = pd.concat(dfs, ignore_index = True)

    df = df[["observation", "value"]]
    tavg = df[df["observation"] == "TAVG"]
    prcp = df[df["observation"] == "PRCP"]
    snow = df[df["observation"] == "SNOW"]

    return tavg, prcp, snow

def main(pop_dir, all_dir, out_dir):
    # https://stackoverflow.com/questions/20906474/import-multiple-csv-files-into-pandas-and-concatenate-into-one-dataframe 
    pop_avg, pop_prcp, pop_snow = get_observs(pop_dir)
    all_avg, all_prcp, all_snow = get_observs(all_dir)

    avg_anovaResults = scipy.stats.f_oneway(pop_avg["value"], all_avg["value"]).pvalue
    prcp_anovaResults = scipy.stats.f_oneway(pop_prcp["value"], all_prcp["value"]).pvalue
    snow_anovaResults = scipy.stats.f_oneway(pop_snow["value"], all_snow["value"]).pvalue

    # avg_melt = pd.melt(all_avg)
    # prcp_melt = pd.melt(all_prcp)
    # snow_melt = pd.melt(all_snow)

    # avg_posthoc = pairwise_tukeyhsd(avg_melt['value'], avg_melt['variable'], alpha=0.05)
    # prcp_posthoc = pairwise_tukeyhsd(avg_melt['value'], avg_melt['variable'], alpha=0.05)
    # snow_posthoc = pairwise_tukeyhsd(avg_melt['value'], avg_melt['variable'], alpha=0.05)

    ttest_rel_avg_pval = scipy.stats.ttest_1samp(pop_avg['value'], all_avg['value'].mean(), alternative = 'two-sided').pvalue
    ttest_rel_prcp_pval = scipy.stats.ttest_1samp(pop_prcp['value'], all_prcp['value'].mean(), alternative = 'two-sided').pvalue
    ttest_rel_snow_pval = scipy.stats.ttest_1samp(pop_snow['value'], all_snow['value'].mean(), alternative = 'two-sided').pvalue

    #df = pd.DataFrame(columns = ["type", "pop_normality", "all_normality", "lvn_pval", "mwn_pval"])
    df = pd.DataFrame(columns = ["type", "ttest_rel_pval", "anova_res"])
    df["type"] = ["avg", "prcp", "snow"]
    df["ttest_rel_pval"] = [ttest_rel_avg_pval, ttest_rel_prcp_pval, ttest_rel_snow_pval]
    df["anova_res"] = [avg_anovaResults, prcp_anovaResults, snow_anovaResults]

    df.to_csv(out_dir)


if __name__ == "__main__":
    pop_dir = sys.argv[1]
    all_dir = sys.argv[2]
    out_dir = sys.argv[3]

    #txt_converter.convert_cities()
    main(pop_dir, all_dir, out_dir)