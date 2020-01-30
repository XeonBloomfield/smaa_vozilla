import logging
import math

from keras.models import load_model
import numpy as np

from geopy.geocoders import Nominatim
from geopy.distance import distance
import osrm



logging.basicConfig(level=logging.INFO)
log = logging.getLogger("demo")


if __name__ == '__main__':
    source = input("Enter source address: ") or "wybrzeże Stanisława Wyspiańskiego 27, 50-370 Wrocław"
    destination = input("Enter destination address: ") or "plac Dominikański 3, 50-159 Wrocław"
    temperature = int(input("Enter current temperature in Celsius degrees: ") or "18")
    startingPct = int(input("Enter current car battery level [%]: ") or "4")

    log.info("Entered source = {}".format(source))
    log.info("Entered destination = {}".format(destination))
    log.info("Entered startingPct = {}".format(startingPct))

    geolocator = Nominatim(user_agent="specify_your_app_name_here", timeout=30)
    sourceData = geolocator.geocode(source)
    destinationData = geolocator.geocode(destination)

    log.info("source address = {}".format(sourceData.address))
    log.info("destination latitude, longitude = {}, {}".format(sourceData.latitude, sourceData.longitude))

    log.info("source address = {}".format(destinationData.address))
    log.info("destination latitude, longitude = {}, {}".format(destinationData.latitude, destinationData.longitude))

    # distanceBetween = distance((sourceData.latitude, sourceData.longitude),
    #                            (destinationData.latitude, destinationData.longitude)).kilometers
    # log.info("distanceBetween = {}".format(distanceBetween))

    client = osrm.Client(host='http://localhost:5000')

    response = client.route(
        coordinates=[[sourceData.longitude, sourceData.latitude],
                     [destinationData.longitude, destinationData.latitude]],
        overview=osrm.overview.full)

    distanceBetween = math.ceil(response['routes'][0]['distance'] / 1000)
    log.info("distanceBetween = {}".format(distanceBetween))

    durationBetween = math.ceil(response['routes'][0]['duration'] / 60)
    log.info("durationBetween = {}".format(durationBetween))

    log.info("=== Loading models ===")
    model1 = load_model('100_in3/multilayer_perceptron_in3.hdf5')
    model2 = load_model('100_in3/regressor_in3.hdf5')

    x = np.array([(distanceBetween, durationBetween, temperature)])
    prediction_multilayer_perceptron = model1.predict(x)
    log.info("prediction_multilayer_perceptron = {}".format(prediction_multilayer_perceptron))

    x = x.reshape((x.shape[0], 3, 1))
    prediction_regressor = model2.predict(x)
    log.info("prediction_regressor = {}".format(prediction_regressor))

    if startingPct - prediction_multilayer_perceptron[0][0] > 0:
        print("Multilayer Perceptron says you can make it! :)")
    else:
        print("Multilayer Perceptron says you won't make it! :(")

    if startingPct - prediction_regressor[0][0] > 0:
        print("LSTM says you can make it! :)")
    else:
        print("LSTM says you won't make it! :(")
