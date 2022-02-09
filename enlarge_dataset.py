import pandas as pd

ds = pd.read_csv("data/geolife_trajectories_complete.csv")

small_ds = ds.loc[ds['user'].isin([i for i in range(25)])]

small_ds['lat']= small_ds['lat']+1
small_ds['lon']= small_ds['lon']+1


frames = [small_ds,ds]

result = pd.concat(frames)

result.to_csv(f"data/enlarged_users.csv", index=False)
