from typing import Tuple
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import shapefile as shp

# Input file
INPUT_FILE_STAYPOINTS: str = 'data/stayPoints/part-00000-79557233-ac4a-4614-a0df-a38a195def1b-c000.csv'

INPUT_FILE_STAYREGIONS = "data/stayRegions/part-00000-91820f99-df2f-4cd7-a730-3a18a08a130e-c000.csv"

# Map file name (both .shp and .dbf)
MAP_FILE_NAME: str = "data/maps/land_limits"

GRID_SIZE = 600 / 111111


def plot_map():
    map_shp_file = open(MAP_FILE_NAME + '.shp', 'rb')
    map_dbf_file = open(MAP_FILE_NAME + '.dbf', 'rb')
    map_file = shp.Reader(shp=map_shp_file, dbf=map_dbf_file)
    plt.figure()
    for shape in map_file.shapeRecords():
        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        plt.plot(x, y)


def plot_points_and_map(stay_points, stay_regions, bounds: Tuple[float, float, float, float]):
    plot_map()

    for _, stay_region in stay_regions.iterrows():
        center_x = stay_region["longitude"]
        center_y = stay_region["latitude"]
        bottom_left_x = center_x - (GRID_SIZE / 2)
        bottom_left_y = center_y - (GRID_SIZE / 2)

        rect = mpatches.Rectangle((bottom_left_x, bottom_left_y), GRID_SIZE, GRID_SIZE, alpha=0.1, facecolor="red")
        plt.gca().add_patch(rect)

    #plt.scatter(stay_regions["longitude"], stay_regions["latitude"], s = 20, color = 'red')
    plt.scatter(stay_points["longitude"], stay_points["latitude"], s = 1, color = 'black')

    plt.xlim(bounds[0], bounds[1])
    plt.ylim(bounds[2], bounds[3])
    plt.title(f"Stay Points: {len(stay_points)} - Stay Regions:{len(stay_regions)}")
    plt.legend()
    plt.show()


def main():
    stay_points = pd.read_csv(INPUT_FILE_STAYPOINTS)
    stay_regions = pd.read_csv(INPUT_FILE_STAYREGIONS)

    stay_points = stay_points.drop(["timeOfArrival", "timeOfLeave"], axis=1)

    #X = np.array(X)

    # Map bounds
    lon_min = -180.0
    lon_max = 180.0
    lat_min = -90.0
    lat_max = 90.0


    bounds = (lon_min, lon_max, lat_min, lat_max)

    plot_points_and_map(stay_points, stay_regions, bounds)


if __name__ == '__main__':
    main()
