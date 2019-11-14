import connection
import config
from datetime import datetime
import random
import time
import uuid


class CarStatus:
    # static
    unique_vehicles = set()
    vehicle_queue = list()
    vehicle_status_queue = list()

    def __init__(self, json_obj, timestamp, cur):
        self.cur = cur
        self.timestamp = timestamp
        self.discriminator = json_obj['discriminator']
        self.platesNumber = json_obj['platesNumber']
        self.sideNumber = json_obj['sideNumber']
        self.color = 'white'
        self.type = json_obj['type'].lower()
        self.rangeKm = int(json_obj['rangeKm'])
        self.batteryLevelPct = int(json_obj['batteryLevelPct'])
        self.reservationEnd = json_obj['reservationEnd']
        self.status = json_obj['status'].lower()
        self.locationDescription = json_obj['locationDescription']
        self.id = json_obj['id']
        self.name = json_obj['name']
        self.description = json_obj['description']
        self.latitude = float(json_obj['location']['latitude'])
        self.longitude = float(json_obj['location']['longitude'])

    def process(self):
        self.add_if_unique_vehicle()
        self.add_status()

    def db_operations(self):
        if len(CarStatus.vehicle_queue) > 0:
            connection.insert(self.cur, config.VEHICLE_TABLE_NAME, CarStatus.vehicle_queue)
        if len(CarStatus.vehicle_status_queue) > 0:
            connection.insert(self.cur, config.VEHICLE_STATUS_TABLE_NAME, CarStatus.vehicle_status_queue)

        CarStatus.vehicle_queue = list()
        CarStatus.vehicle_status_queue = list()

    def __repr__(self):
        return str(vars(self))

    # Vehicle

    def add_if_unique_vehicle(self):
        if self.id not in CarStatus.unique_vehicles:
            self.add_vehicle()
            CarStatus.unique_vehicles.add(self.id)

    def add_vehicle(self):
        values = [self.id, self.type, self.name, self.color, self.platesNumber, self.sideNumber]
        CarStatus.vehicle_queue.append(values)
        # connection.insert(self.cur, config.VEHICLE_TABLE_NAME, values)

    # Vehicle Status

    def add_status(self):
        values = [str(uuid.uuid4()), self.id, self.timestamp, self.status, self.latitude, self.longitude, self.rangeKm,
                  self.batteryLevelPct]
        CarStatus.vehicle_status_queue.append(values)
        # connection.insert(self.cur, config.VEHICLE_STATUS_TABLE_NAME, values)
