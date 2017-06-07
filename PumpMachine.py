import pandas as pd
import datetime
import sched, time


print("***************Fetching Data Round 1************************")
df_total = pd.read_json("https://api.coinmarketcap.com/v1/ticker")
df_1h = df_total[["name", "24h_volume_usd", "percent_change_1h" ]]
df_1h = df_1h.set_index("name")

s = sched.scheduler(time.time, time.sleep)

iteration = 1

def gatherPrices(df, iteration):
    df_total_temp = pd.read_json("https://api.coinmarketcap.com/v1/ticker")
    df_1h_last = df_total_temp[["name", "percent_change_1h"]]
    df_1h_last = df_1h_last.set_index("name")

    if iteration <= 2:
        print("***************Fetching Data Round {}************************".format(iteration+1))
        df = df.join(df_1h_last, rsuffix="_"+str(iteration))
    else:
        print("***************Pumpers************************")
        df = df.join(df_1h_last, rsuffix="_"+str(iteration))
        df = df.drop(df.columns[[1]], axis=1)
        df = df.dropna()
        time.sleep(3)
        pumpersDF = df.ix[(df[df.columns[1]] > 1.00) & (df[df.columns[2]] > 1.00) & (df[df.columns[3]] > 1.00)]
        print(pumpersDF.sort_values(by = pumpersDF.columns[1], ascending=False).head(5))

    iteration += 1

    s.enter(2, 1, gatherPrices, (df,iteration))


s.enter(2, 1, gatherPrices, (df_1h,iteration))
s.run()
