import connection
import pandas
import math
from sklearn.model_selection import train_test_split

from geopy.distance import distance



_query = """
select *
from
(
select r.id, true as "start", false as "end", vs."date", vs.latitude, vs.longitude, vs."rangeKm", vs."batteryLevelPct" 
from "Rent" r
join "VehicleStatus" vs on vs."vehicleId" = r."vehicleId" and vs."date" = r."start"
union
select r.id, false as "start", true as "end", vs."date", vs.latitude, vs.longitude, vs."rangeKm", vs."batteryLevelPct" 
from "Rent" r
join "VehicleStatus" vs on vs."vehicleId" = r."vehicleId" and vs."date" = r."end"
) t
order by
t.id, t."date", t."end"
desc;
"""


def get_raw_data():
    conn, cur = connection.connect()
    result = connection.select(cur, _query)
    connection.close_connection(conn, cur)
    return result


def get_distance(lat_1, lng_1, lat_2, lng_2):
    return distance((lat_1, lng_1), (lat_2, lng_2)).kilometers


def get_eval_data():
    raw_data = get_raw_data()
    raw_df = pandas.DataFrame(raw_data, columns=['id', 's', 'e', 'date', 'lat', 'lon', 'range', 'battery'])
    start = raw_df.query('s')
    end = raw_df.query('e')
    end.columns = ['id', 's2', 'e2', 'date2', 'lat2', 'lon2', 'range2', 'battery2']
    df = start.set_index('id').join(end.set_index('id'))

    df['rangeDiff'] = df.apply(lambda row: row['range'] - row['range2'], axis=1)
    df['batteryPctDiff'] = df.apply(lambda row: row['battery'] - row['battery2'], axis=1)
    df['distance'] = df.apply(lambda row: get_distance(row['lat'], row['lon'], row['lat2'], row['lon2']), axis=1)
    df['timedelta'] = df.apply(lambda row: ((row['date2'] - row['date']).seconds//60)%60, axis=1)

    df[['timedelta', 'rangeDiff', 'batteryPctDiff', 'distance']].to_csv('data/all.csv', index=False)
    x = df[['distance', 'timedelta']]
    y = df['batteryPctDiff']
    xtrain, xrest = train_test_split(x, test_size=0.2)
    xvalid, xtest = train_test_split(xrest, test_size=0.5)
    ytrain, yrest = train_test_split(y, test_size=0.2)
    yvalid, ytest = train_test_split(yrest, test_size=0.5)

    xtrain.to_csv('data/xtrain.csv', index=False)
    xvalid.to_csv('data/xvalid.csv', index=False)
    xtest.to_csv('data/xtest.csv', index=False)
    ytrain.to_csv('data/ytrain.csv', index=False)
    yvalid.to_csv('data/yvalid.csv', index=False)
    ytest.to_csv('data/ytest.csv', index=False)




if __name__ == '__main__':
    get_eval_data()
