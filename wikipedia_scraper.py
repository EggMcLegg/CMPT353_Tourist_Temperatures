import pandas as pd
from pathlib import Path

# https://www.geeksforgeeks.org/scraping-wikipedia-table-with-pandas-using-read_html/
# scraping learned from above
def scrape():
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_cities_by_international_visitors")

    cities = df[0]['City']
    countries = df[0]['Country / Territory']
    EU_num_arr = df[0]['Arrivals 2018 (Euromonitor)']
    EU_growth = df[0]['Growth in arrivals (Euromonitor)']
    #print(EUarr)
    MC_num_arr = df[0]['Arrivals 2016 (Mastercard)']

    df = pd.DataFrame({'City': cities, 
                       'Country': countries,
                       'Arrivals 2016 (Mastercard)': MC_num_arr,
                       'Arrivals 2018 (Euromonitor)': EU_num_arr,
                       'Arrivals 2018 Growth (Euromonitor)': EU_growth})

    df = df.dropna()

    #print(EU_df)
    #print(MC_df)

    fp = Path('processed_data/city_counts.csv')  

    fp.parent.mkdir(parents = True, exist_ok = True)  

    df.to_csv(fp, index = False)