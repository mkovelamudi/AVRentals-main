from flask import Flask, jsonify, request
import yaml
import carla
import matplotlib.pyplot as plt
from pymongo import MongoClient
from models.World import World
import uuid
import json
import time

AVAILABLE_CAR_BRAND = ['audi','mercedes', 'chevrolet', 'tesla', 'dodge', 'ford', 'lincoln','mini','volkswagen','toyota','nissan','bmw']
AVAILABLE_CAR_REGEX = 'vehicle.(' + '|'.join(AVAILABLE_CAR_BRAND) + ').*'
WAYPOINT_TO_MILES_RATIO = 1/400
DEFAULT_NEARBY_CAR_COUNT = 5

def main():
    config = open('config.yaml', 'r')
    args = yaml.safe_load(config)
    carla_args = args['Carla']
    mongo_args = args['Mongo']

    mongo_client = MongoClient("mongodb+srv://%s:%s@cmpe281.icmh6.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        % (mongo_args['username'], mongo_args['password']))

    carla_client = carla.Client(carla_args['host'], carla_args['port'])
    carla_world = carla_client.get_world()
    apply_world_settings(carla_world, carla_args)
    world = World(carla_world, mongo_client.get_database('carla'))

    while True:
        choice = input("Enter your input (A/R/Q/S/T/D/step): ")

        if choice == 'Q':
            world.kill_all_threads()
            break
        elif choice == 'A':
            vehicle_id = uuid.uuid1().hex
            world.add_vehicle(vehicle_id)
            print("Successfully create vehicle: ", vehicle_id)
        elif choice == 'R':
            vehicle_id = input("Enter vehicle id to be removed: ")
            print("Removed vehicle: ", world.remove_vehicle(vehicle_id))
        elif choice == 'S':
            sp = world.get_random_spawn_point().location
            print(world.get_nearest_vehicles(sp, 5))
        elif choice == 'T':
            sp = world.get_random_spawn_point().location
            vehicle_id = input("Enter vehicle id to be reroute: ")
            world.trip_init(vehicle_id, sp)
        elif choice == 'D':
            vehicle_id = input("Enter vehicle id to be reroute: ")
            print(world.get_arrive(vehicle_id))
        elif choice == 'W':
            world.world.wait_for_tick(5)
        elif choice == 'step':
            vehicle_id = input("Enter vehicle id to run one step: ")
            world.run_step(vehicle_id)

def apply_world_settings(world, config):
    world_config = config['World']
    settings = world.get_settings()
    # for key in world_config.keys():
    #     settings[key] = world_config[key]
    settings.fixed_delta_seconds = world_config['fixed_delta_seconds']
    world.apply_settings(settings)




config = open('config.yaml', 'r')
args = yaml.safe_load(config)
carla_args = args['Carla']
mongo_args = args['Mongo']
location_args = args['Locations']
node_args = args['Node']

mongo_client = MongoClient("mongodb+srv://%s:%s@%s/myFirstDatabase?retryWrites=true&w=majority"
    % (mongo_args['username'], mongo_args['password'], mongo_args['uri']))

carla_client = carla.Client(carla_args['host'], carla_args['port'])
carla_world = carla_client.get_world()
apply_world_settings(carla_world, carla_args)
world = World(
    carla_world,
    mongo_client.get_database(mongo_args['database']),
    node_args['url']
)



app = Flask(__name__)

@app.route('/vehicle', methods=['POST'])
def add_vehicle():
    form = json.loads(request.get_data())
    if 'vehicle_id' not in form:
        return "Please include vehicle_id in the body", 400
    vehicle_id = int(form['vehicle_id'])

    try:
        vehicle_info = world.add_vehicle(vehicle_id)
        return jsonify(vehicle_info), 201

    except:
        return 'Vehicle id already exist', 409

@app.route('/vehicle/<vehicle_id>/trip', methods=['GET'])
def get_vehicle_trip(vehicle_id):
    trip = world.get_vehicle_trip(int(vehicle_id))
    if trip:
        return jsonify(trip), 200
    return "No in progress trip", 404

@app.route('/vehicle/<vehicle_id>', methods=['GET', 'DELETE'])
def get_vehicle(vehicle_id):
    vehicle_id = int(vehicle_id)
    vehicle_info = world.get_vehicle(vehicle_id)
    if not vehicle_info:
        return "Vehicle does not exist", 404

    if request.method == 'GET':
        return jsonify(vehicle_info), 200
    else:
        if world.remove_vehicle(vehicle_id):
            return "Successfully removed", 200
        else:
            return "Failed to remove vehicle", 500

@app.route('/vehicle/all', methods=['GET'])
def get_all_vehicles():
    return jsonify(world.get_all_vehicles()), 200

#location=lN
@app.route('/trip/nearby', methods=["GET"])
def get_nearby_vehicles():
    location = request.args.get('location')
    carla_location = location_to_carla_spawnpoint(location)
    if not carla_location:
        return "Incorrect location", 400

    nearby_cars = world.get_nearest_vehicles(
        carla_location,
        DEFAULT_NEARBY_CAR_COUNT
    )

    # Convert waypoints to miles
    for c in nearby_cars:
        c['distance'] *= WAYPOINT_TO_MILES_RATIO

    return jsonify(nearby_cars), 200

@app.route('/trip/init', methods=['POST'])
def initiate_trip():
    form = json.loads(request.get_data())
    print(form)
    if 'vehicle_id' not in form or \
       'trip_id' not in form or \
       'pickup_location' not in form or \
       'destination' not in form:
        return "Missing required parameter", 400

    vehicle_id = form['vehicle_id']
    trip_id = form['trip_id']
    pickup_location = form['pickup_location']
    destination = form['destination']

    crash = bool(form['crash']) if 'crash' in form else False

    pickup_sp = location_to_carla_spawnpoint(pickup_location)
    destination_sp = location_to_carla_spawnpoint(destination)

    if not pickup_sp:
        return "Pickup location is not in correct format. Should be 'l1'~'l10'", 400
    if not destination_sp:
        return "Destination location is not in correct format. Should be 'l1'~'l10'", 400

    try:
        error = world.trip_init(
            vehicle_id,
            trip_id,
            pickup_sp,
            destination_sp
        )
        print("Init error: ", error)
        if error:
            return error, 400
    except:
        return "Trip id already exists", 409

    try:
        waypoint_count = world.trip_to_pickup(trip_id, crash)

        return jsonify({
            "pickup_eta": waypoint_count_to_eta(waypoint_count)
        }), 200
    except:
        return "Trip not in correct status", 400


@app.route('/trip/pickup', methods=['POST'])
def trip_to_des():
    form = json.loads(request.get_data())
    if 'trip_id' not in form:
        return "Missing required parameter trip_id", 400

    trip_id = form['trip_id']
    crash = bool(form['crash']) if 'crash' in form else False

    try:
        waypoint_count = world.trip_to_destination(trip_id, crash)
        return jsonify({
            "destination_eta": waypoint_count_to_eta(waypoint_count)
        }), 200

    except:
        return "Trip not in correct status", 400


# trip_id: trip_id
@app.route('/trip/status/<trip_id>')
def trip_has_reached(trip_id):
    trip_id = int(trip_id)
    try:
        status = world.trip_status(trip_id)
        if 'eta' in status:
            status['eta'] = waypoint_count_to_eta(status['eta'])
        return jsonify(status), 200
    except:
        return "Trip does not exist", 404

@app.route('/resetall', methods=['DELETE'])
def reset_all():
    form = json.loads(request.get_data())
    timestamp = form['timestamp']
    current_time = time.localtime()
    timecode = str(current_time.tm_hour) + str(current_time.tm_min)
    print (timestamp, timecode)
    if timestamp == timecode:
        world.reset_all_vehicles_and_trips()
        return "Successfully remove everything", 200
    return "Bad request", 400


def waypoint_count_to_eta(waypoint_count):
    return 5*waypoint_count

def location_to_carla_spawnpoint(location):
    if location not in location_args:
        return None
    return location_args[location]