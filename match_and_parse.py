import uuid
import connection
import random
import datetime
import logging as log

log.basicConfig(level=log.WARN)
class ChargeStat:

    def __init__(self, vid, sid, start, batteryPctStart):
        self.vehicleId = vid
        self.servicemandId = sid
        self.chargingStart = start
        self.chargingEnd = None
        self.batteryPctStart = batteryPctStart
        self.batteryPctEnd = None

    def set_charging_end(self, end):
        if end < self.chargingStart:
            raise IOError('End of charging cannot be ealier than start')
        self.chargingEnd = end

    def set_battery_Pct_end(self, end):
        self.batteryPctEnd = end
    
    def insert_to_db(self, cur):
        values = [str(uuid.uuid4()), self.vehicleId, self.servicemandId, self.chargingStart, self.chargingEnd, self.batteryPctEnd - self.batteryPctStart]
        return values

class CarStatusFromDB:
    def __init__(self, data):
        # id, "vehicleId", "date", status, latitude, longitude, "rangeKm", "batteryLevelPct"
        self.date = data[0]
        self.batteryLevelPct = data[1]

class VehicleChargingLoader():

    def __init__(self, cur):
        self.cur = cur
    
    def get_serviceman_ids(self):
        query = 'SELECT "personId" FROM "Serviceman";'
        return list(map(lambda x: x[0], connection.select(self.cur, query)))

    def get_vehicle_ids(self):
        query = 'SELECT id FROM "Vehicle";'
        return list(map(lambda x: x[0], connection.select(self.cur, query)))

    def get_vehicle_charging_statuses(self, vehicle_id):
        query = 'SELECT "date", "batteryLevelPct" FROM "VehicleStatus" where "vehicleId"=\'{vid}\' and status = \'unavailable\' order by "date";'.format(vid=vehicle_id)
        return list(map(lambda x: CarStatusFromDB(x), connection.select(self.cur, query)))
    

    def parse_vehicle_charging(self, sids, vid):
        bi = list()
        chargStat = self.get_vehicle_charging_statuses(vid)
        chargeStatObj = ChargeStat(vid, random.choice(sids), chargStat[0].date, chargStat[0].batteryLevelPct)
        for i in range(len(chargStat)-2):
            if i == len(chargStat) - 3:
                chargeStatObj.set_charging_end(chargStat[i].date)
                chargeStatObj.set_battery_Pct_end(chargStat[i].batteryLevelPct)
                bi.append(chargeStatObj.insert_to_db(self.cur))
                break
            if chargStat[i].date + datetime.timedelta(hours=1) < chargStat[i+1].date:
                chargeStatObj.set_charging_end(chargStat[i].date)
                chargeStatObj.set_battery_Pct_end(chargStat[i].batteryLevelPct)
                bi.append(chargeStatObj.insert_to_db(self.cur))
                chargeStatObj = ChargeStat(vid, random.choice(sids), chargStat[i+1].date, chargStat[i+1].batteryLevelPct)
        connection.insert(self.cur, 'VehicleCharging', bi)



if __name__ == "__main__":
    conn, cur = connection.connect()
    vrlap = VehicleChargingLoader(cur)
    uids = vrlap.get_serviceman_ids()
    vids = vrlap.get_vehicle_ids()
    i = 0
    for vid in vids:
        log.warn(vid + ': ' + str(i) + '/' + str(len(vids)))
        vrlap.parse_vehicle_charging(uids, vid)
        conn.commit()
        i += 1
    connection.close_connection(conn, cur)
