import pandas

NUMBER_OF_USERS = 18

ds = pandas.read_csv("data/geolife_trajectories_complete.csv")
small_ds = ds.loc[ds['user'].isin([i for i in range(NUMBER_OF_USERS)])]
small_ds.to_csv(f"data/{NUMBER_OF_USERS}_users.csv")
