import uuid
import connection
import random
import datetime
import logging as log

log.basicConfig(level=log.WARN)


class CarStat:

    def __init__(self, vid, fkid, stype, start):
        self.id = str(uuid.uuid4())
        self.vehicleId = vid
        self.FkId = fkid
        self.status_type = stype
        self.start = start
        self.end = None

    def set_end(self, end):
        if end < self.start:
            raise ValueError('End cannot be ealier than start: ' + str(self.start) + ' | ' + str(end))
        self.end = end

    def calcEstEnd(self):
        if self.start + datetime.timedelta(minutes=5) < self.end:
            return self.start + datetime.timedelta(minutes=15)
        return self.start + datetime.timedelta(minutes=5)

    def calcKmMade(self):
        return (self.end - self.start).seconds // 92

    def insert_to_db(self, cur):
        if self.status_type == 'reserved':
            values = [self.id, self.vehicleId, self.FkId,
                      self.start, self.calcEstEnd(), self.end]
            return values
        elif self.status_type == 'rented':
            values = [self.id, self.vehicleId, self.FkId,
                      self.start, self.end, self.calcKmMade()]
            return values
        else:
            raise ValueError("Incorrect value of status: {incorrectValue}".format(
                incorrectValue=self.status_type))


class CarStatusFromDB:
    def __init__(self, data):
        # id, "vehicleId", "date", status, latitude, longitude, "rangeKm", "batteryLevelPct"
        self.id = data[0]
        self.date = data[1]
        self.status = data[2]


class VehicleRentLoaderAndPropagator():

    def __init__(self, cur):
        self.cur = cur

    def get_User_ids(self):
        query = 'SELECT "personId" FROM "User";'
        return list(map(lambda x: x[0], connection.select(self.cur, query)))

    def get_vehicle_ids(self):
        query = 'SELECT "id" FROM "Vehicle";'
        res = list(map(lambda x: x[0], connection.select(self.cur, query)))
        return res

    def get_vehicle_reserv_and_rent_statuses(self, vehicle_id):
        query = 'SELECT id, "date", status FROM "VehicleStatus" where "vehicleId"=\'{vid}\' and ("status" = \'reserved\' or "status" = \'rented\') order by "date";'.format(
            vid=vehicle_id)
        res = list(map(lambda x: CarStatusFromDB(
            x), connection.select(self.cur, query)))
        log.info('Loaded {n} car statuses.'.format(n=str(len(res))))
        return res

    def parse_vehicle_resrv_and_rent(self, uids, vid):
        bulk_insert_rent = list()
        bulk_insert_reservation = list()
        carStat = self.get_vehicle_reserv_and_rent_statuses(vid)
        if carStat[0].status == 'rented':
            artificialReservation = CarStat(vid, random.choice(
                uids), 'reserved', carStat[0].date - datetime.timedelta(minutes=random.randint(1, 5)))
            artificialReservation.set_end(carStat[0].date)
            bulk_insert_reservation.append(artificialReservation.insert_to_db(self.cur))
            rid = artificialReservation.id
            carStatObj = CarStat(vid, rid, carStat[0].status, carStat[0].date)
        else:
            carStatObj = CarStat(vid, random.choice(
                uids), carStat[0].status, carStat[0].date)
        for i in range(len(carStat)-2):
            if i == len(carStat) - 3:
                carStatObj.set_end(carStat[i].date)
                if carStat[i].status == 'reserved':
                    bulk_insert_reservation.append(carStatObj.insert_to_db(self.cur))
                elif carStat[i].status == 'rented':
                    bulk_insert_rent.append(carStatObj.insert_to_db(self.cur))
                break
            if carStat[i].date + datetime.timedelta(minutes=2) < carStat[i+1].date:
                carStatObj.set_end(carStat[i].date)
                if carStat[i].status == 'reserved':
                    bulk_insert_reservation.append(carStatObj.insert_to_db(self.cur))
                elif carStat[i].status == 'rented':
                    bulk_insert_rent.append(carStatObj.insert_to_db(self.cur))
                if carStat[i+1].status == 'rented':
                    artificialReservation = CarStat(vid, random.choice(
                        uids), 'reserved', carStat[0].date - datetime.timedelta(minutes=random.randint(1, 5)))
                    artificialReservation.set_end(carStat[0].date)
                    bulk_insert_reservation.append(artificialReservation.insert_to_db(self.cur))
                    rid = artificialReservation.id
                    carStatObj = CarStat(
                        vid, rid, carStat[i+1].status, carStat[i+1].date)
                else:
                    carStatObj = CarStat(vid, random.choice(
                        uids), carStat[i+1].status, carStat[i+1].date)
            elif carStat[i].status == 'reserved' and carStat[i+1].status == 'rented':
                carStatObj.set_end(carStat[i].date)
                bulk_insert_reservation.append(carStatObj.insert_to_db(self.cur))
                rid = carStatObj.id
                carStatObj = CarStat(
                    vid, rid, carStat[i+1].status, carStat[i+1].date)
            elif carStat[i].status == 'rented' and carStat[i+1].status == 'reserved':
                carStatObj.set_end(carStat[i].date)
                bulk_insert_rent.append(carStatObj.insert_to_db(self.cur))
                carStatObj = CarStat(vid, random.choice(
                    uids), carStat[i+1].status, carStat[i+1].date)
            
        connection.insert(self.cur, 'Reservation', bulk_insert_reservation)
        connection.insert(self.cur, 'Rent', bulk_insert_rent)
        


if __name__ == "__main__":
    conn, cur = connection.connect()
    vrlap = VehicleRentLoaderAndPropagator(cur)
    uids = vrlap.get_User_ids()
    vids = vrlap.get_vehicle_ids()
    i = 0
    for vid in vids:
        log.warn(vid + ': ' + str(i) + '/' + str(len(vids)))
        vrlap.parse_vehicle_resrv_and_rent(uids, vid)
        conn.commit()
        i += 1
    connection.close_connection(conn, cur)
