import math
import random
import time
import carla
import datetime
import logging
import weakref
import collections
import requests
from agents.navigation.basic_agent import BasicAgent
from agents.navigation.behavior_agent import BehaviorAgent
from .StoppableThread import StoppableThread

SPAWNING_RETRIES = 15

TICK_FREQUENCY = 0.05
STALE_ERROR_OUT = 30 #30 seconds
WAYPOINT_TO_MILES_RATIO = 1/400

STALE_THRESHOLD = STALE_ERROR_OUT / TICK_FREQUENCY # 30 seconds

AVAILABLE_CAR_BRAND = ['audi','mercedes', 'chevrolet', 'tesla', 'dodge', 'ford', 'lincoln','mini','volkswagen','toyota','nissan','bmw']
VEHICLE_ID = "vehicle_id"
TRIP_ID = "trip_id"
CARLA_VEHICLE_ID = "carla_actor_id"

CARLA_STOP_DISTANCE = 8

TRIP_STATUS = {
    0: 'INITIATED',
    1: 'TO_PICKUP',
    2: 'TO_DESTINATION',
    3: 'Completed'
}


class World(object):
    def __init__ (self, carla_world, mongo_client, node_url):
        self.world = carla_world
        self.spawn_points = self.world.get_map().get_spawn_points()
        self.vehicle_bps = []


        for c in AVAILABLE_CAR_BRAND:
            for v in self.world.get_blueprint_library().filter('vehicle.*'):
                if c in v.id:
                    self.vehicle_bps.append(v)

        self.mongo_db = mongo_client
        self.mongo_db.vehicles.create_index('vehicle_id', unique=True)
        self.mongo_db.trips.create_index('trip_id', unique=True)

        self.node_url = node_url

        self.trips = {}

    def add_vehicle(self, vehicle_id, spawn_point_index=None):
        vehicle_bp = random.choice(self.vehicle_bps)
        existing = self.mongo_db.vehicles.find_one({VEHICLE_ID: vehicle_id})
        if existing:
            raise RuntimeError("Vehicle already exist")

        for i in range(SPAWNING_RETRIES):
            if spawn_point_index:
                vehicle_spawn_point = self.spawn_points[spawn_point_index] 
            else:
                vehicle_spawn_point = self.get_random_spawn_point()

            sim_vehicle = self.world.try_spawn_actor(vehicle_bp, vehicle_spawn_point)
            
            # Retry if spawn failed
            if sim_vehicle is None:
                continue

            sim_vehicle.set_autopilot(False)
            break

        if sim_vehicle == None:
            raise RuntimeError('Failed to create vehicle in carla')

        sim_vehicle.set_autopilot(True)

        self.mongo_db.vehicles.insert_one(create_vehicle_record(vehicle_id, sim_vehicle))

        return get_carla_vehicle_info(vehicle_id, sim_vehicle)
    
    def remove_vehicle(self, vehicle_id):
        carla_vehicle = self.get_carla_vehicle_actor(vehicle_id)

        if carla_vehicle is None:
            print('Failed to find the vehicle in carla')
            return False
        
        logging.info('Successfully destroyed: ', carla_vehicle.destroy())

        self.mongo_db.vehicles.update_one({VEHICLE_ID: vehicle_id}, {"$set":{"destroyed": True}})
        return True

    # Return a list of (vehicle_id, location(x,y))
    def get_nearest_vehicles(self, spawn_point_index, number_of_vehicles):
        if number_of_vehicles <= 0:
            return []

        vehicle_records = list(self.mongo_db.vehicles.find({"destroyed": False}))
        carla_vehicles = self.world.get_actors([v[CARLA_VEHICLE_ID] for v in vehicle_records])
        target_location = self.spawn_points[spawn_point_index].location
        distances = [
            (i, get_distance(target_location, carla_vehicles.__getitem__(i).get_location()))
            for i in range(len(carla_vehicles))
        ]

        distances.sort(key=lambda x:x[1])
        closest_vehicle_indexes = [d[0] for d in distances[:number_of_vehicles]]

        results = []

        for i in closest_vehicle_indexes:
            carla_vehicle = carla_vehicles.__getitem__(i)
            route_distance = self.get_waypoint_to_location(carla_vehicle, target_location)
            results.append({
                "vehicle_id": vehicle_records[i][VEHICLE_ID], 
                "current_location": location_to_string(carla_vehicle.get_location()),
                "distance": route_distance, 
                "car_type": carla_vehicle.type_id
            })

        results.sort(key=lambda x: x['distance'])

        return results

    def get_vehicle_trip(self, vehicle_id):
        trips = self.mongo_db.trips.find({VEHICLE_ID: vehicle_id})
        for t in trips:
            if t['status'] == TRIP_STATUS[2]:
                trip_id = t[TRIP_ID]
                if trip_id in self.trips and self.trips[trip_id].is_alive():
                    del t['_id']
                    return t
            else: 
                return t

        return None

    def trip_init(self, vehicle_id, trip_id, pickup_index, destination_index): 
        in_progress_trip = self.mongo_db.trips.find_one({
            "vehicle_id": vehicle_id,
            "status": {
                "$ne": TRIP_STATUS[2]
            }
        })
        if in_progress_trip:
            return "Vehicle currently in another trip: %d" % (in_progress_trip['trip_id'])


        try:
            actor = self.get_carla_vehicle_actor(vehicle_id)
        except:
            return "Vehicle with id [%s] does not exist" % (vehicle_id)

        actor.set_autopilot(False)
        actor.apply_control(carla.VehicleControl(brake=1.0, throttle=0))

        self.mongo_db.trips.insert_one(create_trip_record(
            vehicle_id,
            trip_id,
            pickup_index,
            destination_index,
        ))


    def trip_to_pickup(self, trip_id, crash):
        trip = self.mongo_db.trips.find_one({TRIP_ID: trip_id})
        if trip['status'] != TRIP_STATUS[0]:
            raise RuntimeError('Trip not in standby status')
        
        agent = self.get_carla_agent(trip['vehicle_id'])
        pickup_location = self.spawn_points[trip['pickup_index']].location

        agent.set_destination(pickup_location)    
        waypoints_length = get_remaining_waypoint_count(agent)

        def completion_cb():            
            print("Calling to pickup completion callback")
            requests.put(self.node_url + '/trip/edit/' + str(trip_id), {
                'atPickUp': '1',
            })


        new_thread = StoppableThread(target=self.trace_route, args=(
            trip['vehicle_id'], 
            agent, 
            pickup_location,
            trip_id, 
            crash,
            completion_cb
        ))

        self.trips[trip_id] = new_thread
        new_thread.start()

        self.mongo_db.trips.update_one({TRIP_ID: trip_id}, {"$set": {
            'status': TRIP_STATUS[1]
        }})

        return waypoints_length
            
    def trip_to_destination(self, trip_id, crash):
        trip = self.mongo_db.trips.find_one({TRIP_ID: trip_id})
        if trip['status'] != TRIP_STATUS[1]:
            raise RuntimeError('Car is not in correct status')
        elif self.trips[trip_id].is_alive():
            raise RuntimeError('Car has not reached pickup location')

        agent = self.get_carla_agent(trip['vehicle_id'])
        destination_location = self.spawn_points[trip['destination_index']].location

        agent.set_destination(destination_location)    
        waypoints_length = get_remaining_waypoint_count(agent)

        def completion_cb():   
            print("Calling to destination completion callback")

            vehicle = agent._vehicle       
            completed_trip = self.mongo_db.trips.find_one({TRIP_ID: trip_id})

            requests.put(self.node_url + '/trip/edit/' + str(trip_id), {
                'iscompleted': '1',
                'miles': completed_trip['miles']
            })

            time.sleep(10)
            vehicle.set_autopilot(True)
            print("Set vehicle back to autopilot: ", vehicle.id)

        

        new_thread = StoppableThread(target=self.trace_route, args=(
            trip['vehicle_id'], 
            agent, 
            destination_location,
            trip_id,
            crash,
            completion_cb
        ))
        
        self.mongo_db.trips.update_one({TRIP_ID: trip_id}, {"$set": {
            'status': TRIP_STATUS[2],
            'miles': WAYPOINT_TO_MILES_RATIO * waypoints_length
        }})

        self.trips[trip_id] = new_thread
        new_thread.start()

        return waypoints_length

    def trip_status(self, trip_id):
        trip = self.mongo_db.trips.find_one({TRIP_ID: trip_id})
        print(TRIP_ID, trip_id, type(trip_id), trip)
        if not trip:
            raise LookupError("Trip id does not exist")


        print(self.trips[trip_id])
        if trip['status'] == TRIP_STATUS[0]:
            return {
                "status": 'STANDBY'
            }
        
        incident = self.check_collision(trip['trip_id'], trip['vehicle_id'])

        if trip['status'] == TRIP_STATUS[1]:
            if self.trips[trip_id].is_alive():
                eta = self.check_eta(trip)
                return {
                    "status":'TO_PICKUP',
                    "eta": eta,
                    "incident": incident
                }
            else:
                return {
                    'status': 'AT_PICKUP',
                    'incident': incident
                }
        else:
            if self.trips[trip_id].is_alive():
                eta = self.check_eta(trip)
                return {
                    'status': 'TO_DESTINATION',
                    'eta': eta,
                    'incident': incident
                }
            else:
                return {
                    'status': 'FINISHED'
                }

    def trace_route(
        self, 
        vehicle_id, 
        agent, 
        destination, 
        trip_id, 
        crash, 
        completion_cb
    ):
        waypoints_queue = agent.get_local_planner()._waypoints_queue
        vehicle = agent._vehicle

        def log_collision(collision_message):
            self.mongo_db.collision_log.insert_one({
                "vehicle_id": vehicle_id,
                "trip_id": trip_id,
                "message": collision_message,
                "timestamp": get_current_timestamp()
        })

        collision_sensor = CollisionSensor(vehicle, log_collision)

        iteration_counter = 0 

        if crash:
            iterations = int(10 / TICK_FREQUENCY)
            for i in range(iterations):
                vehicle.apply_control(carla.VehicleControl(brake=0, throttle=1, steer=0.1))
                self.log_vehicle_info_to_db(vehicle_id, trip_id, vehicle, collision_sensor)
                time.sleep(TICK_FREQUENCY)
                iteration_counter += 1

                if iteration_counter % 10 == 0:
                    incident = self.check_collision(trip_id, vehicle_id)
                    if incident:
                        self.log_incident_in_node(trip_id, incident)

            return

        stale_count = {}
        while len(waypoints_queue) > CARLA_STOP_DISTANCE:
            waypoints_left = len(waypoints_queue)
            if waypoints_left in stale_count:
                stale_count[waypoints_left] += 1
            else:
                stale_count[waypoints_left] = 1

            if stale_count[waypoints_left] > STALE_THRESHOLD:
                logging.error("Vehicle [%d] stale for more than %f seconds. Teleport to \
                    new starting point" % 
                    (vehicle.id, STALE_THRESHOLD/10))
                self.random_teleport_vehicle(vehicle)
                agent.set_destination(destination)
                vehicle.apply_control(carla.VehicleControl(brake=1.0, throttle=0, steer=0))
                waypoints_queue = agent.get_local_planner()._waypoints_queue
                stale_count = {}
                continue
            
            self.log_vehicle_info_to_db(vehicle_id, trip_id, vehicle, collision_sensor)

            control = agent.run_step()
            vehicle.apply_control(control)
            iteration_counter += 1

            if iteration_counter % 10 == 0:
                incident = self.check_collision(trip_id, vehicle_id)
                if incident:
                    self.log_incident_in_node(trip_id, incident)
                    return

            if iteration_counter % int(1/TICK_FREQUENCY) == 0:
                self.one_second_cb(trip_id)

            time.sleep(TICK_FREQUENCY)
        
        vehicle.apply_control(carla.VehicleControl(brake=0))
        logging.info("Destination reached", {'vehicle_id': vehicle.id})

        self.update_trip_in_db(trip_id)
        collision_sensor.destroy()
        completion_cb()
            
    def one_second_cb(self, trip_id):
        newest_trip = self.mongo_db.trips.find_one({TRIP_ID: trip_id})

        requests.put(self.node_url + '/trip/edit/' + str(trip_id), {
            'eta': self.check_eta(newest_trip)
        })

    def log_vehicle_info_to_db(self, vehicle_id, trip_id, vehicle, collision_sensor): 
        frame = self.world.get_snapshot().frame

        vel = vehicle.get_velocity()
        transform = vehicle.get_transform()
        control = vehicle.get_control()
        heading = 'N' if abs(transform.rotation.yaw) < 89.5 else '' 
        heading += 'S' if abs(transform.rotation.yaw) > 90.5 else ''
        heading += 'E' if 179.5 > transform.rotation.yaw > 0.5 else ''
        heading += 'W' if -0.5 > transform.rotation.yaw > -179.5 else ''
        colhist = collision_sensor.get_collision_history()
        collision = [colhist[x + frame - 200] for x in range(0, 200)]

        self.mongo_db.vehicle_log.insert_one({
            "vehicle_id": vehicle_id,
            "trip_id": trip_id,
            "Speed (km/h)": (3.6 * math.sqrt(vel.x**2 + vel.y**2 + vel.z**2)),
            "Heading": transform.rotation.yaw,
            "Heading Direction": heading,
            "Location x": transform.location.x,
            "Location y": transform.location.y,
            "Throttle": (control.throttle, 0.0, 1.0),
            "Steer": (control.steer, -1.0, 1.0),
            "Brake": (control.brake, 0.0, 1.0),
            "Reverse": control.reverse,
            "Collision": collision,
            "timestamp": get_current_timestamp()
        })


    def update_trip_in_db(self, trip_id):
        trip = self.mongo_db.trips.find_one({TRIP_ID:trip_id})
        if trip['status'] == TRIP_STATUS[1]:
            self.mongo_db.trips.update_one({TRIP_ID: trip_id}, {"$set": {
                "pickup_time": get_current_timestamp()
            }})
        elif trip['status'] == TRIP_STATUS[2]:
            self.mongo_db.trips.update_one({TRIP_ID: trip_id}, {"$set": {
                "dropoff_time": get_current_timestamp()
            }})

    def run_step(self, vehicle_id):
        agent = self.get_carla_agent(vehicle_id)
        agent.run_step()
        print("Run 1 step")

    def get_waypoint_to_location(self, carla_vehicle, destination):
        agent = BasicAgent(carla_vehicle)
        agent.set_destination(destination)
        return get_remaining_waypoint_count(agent)


    def check_eta(self, trip):        
        if trip['status'] == TRIP_STATUS[1]:
            target_location = self.spawn_points[trip['pickup_index']].location
        else: 
            target_location = self.spawn_points[trip['destination_index']].location 

        return waypoint_count_to_eta(self.get_waypoint_to_location(
            self.get_carla_vehicle_actor(trip['vehicle_id']),
            target_location    
        ))

    def check_collision(self, trip_id, vehicle_id):
        vehicle_log = list(self.mongo_db.vehicle_log.find({
            "trip_id": trip_id,
            "vehicle_id": vehicle_id
        }).sort("timestamp", -1).limit(1))

        if len(vehicle_log) > 0:
            for x in vehicle_log[0]['Collision']:
                if x[0] > 0:
                    return x[1]

    def log_incident_in_node(self, trip_id, incident):
        requests.put(self.node_url + '/trip/edit/' + str(trip_id), {
            'collision': incident
        })


    def get_random_spawn_point(self):
        return random.choice(self.spawn_points)

    def get_carla_vehicle_actor(self, vehicle_id):
        vehicle_record = self.mongo_db.vehicles.find_one({VEHICLE_ID: vehicle_id})
        if vehicle_record is None:
            raise RuntimeError('Failed to find the vehicle in database')

        carla_actor_id = vehicle_record[CARLA_VEHICLE_ID]
        carla_actor = self.world.get_actor(carla_actor_id)

        if carla_actor is None:
            raise RuntimeError('Failed to find the vehicle in Carla')


        return carla_actor

    def get_carla_agent(self, vehicle_id):
        carla_actor = self.get_carla_vehicle_actor(vehicle_id)
        return BehaviorAgent(carla_actor)

    def kill_all_threads(self):
        for t in self.trips.values:
            t.stop()
            t.join()
    
    def get_next_des_and_advance(trip): 
        if trip['thread'].is_alive():
            return None
        else:
            if trip['status'] == TRIP_STATUS[0]:
                trip['status'] = TRIP_STATUS[1]
                return trip['']

    def random_teleport_vehicle(self, vehicle):
        vehicle_spawn_point = self.get_random_spawn_point()
        vehicle.set_transform(vehicle_spawn_point)

    def get_vehicle(self, vehicle_id):
        try:
            vehicle = self.get_carla_vehicle_actor(vehicle_id)
            return get_carla_vehicle_info(vehicle_id, vehicle)
        except:
            return None

    def get_all_vehicles(self):
        vehicle_records = list(self.mongo_db.vehicles.find({"destroyed": False}))
        results = []
        for v in vehicle_records:
            actor = self.world.get_actor(v['carla_actor_id'])
            if actor:
                results.append(get_carla_vehicle_info(v['vehicle_id'],actor))
            else:
                logging.warning('Missing carla vehicle', extra={
                    "vehicle_id": v['vehicle_id'],
                    "carla_id": v['carla_actor_id']
                })
        return results
    
    def reset_all_vehicles_and_trips(self):
        all_vehicles = self.world.get_actors().filter('vehicle.*')
        for car in all_vehicles:
            car.destroy()
        self.mongo_db.vehicles.delete_many({})
        self.mongo_db.trips.delete_many({})
    
def get_current_timestamp():
    return datetime.datetime.now().isoformat()

def create_vehicle_record(vehicle_id, carla_vehicle):
    return {
        VEHICLE_ID: vehicle_id,
        CARLA_VEHICLE_ID: carla_vehicle.id,
        "type_id": carla_vehicle.type_id,
        "created": get_current_timestamp(),
        "updated": get_current_timestamp(),
        "destroyed": False
    }

def create_trip_record(vehicle_id, trip_id, pickup_index, destination_index):
    return {
        VEHICLE_ID: vehicle_id,
        TRIP_ID: trip_id,
        'pickup_index': pickup_index,
        'destination_index': destination_index,
        'status': TRIP_STATUS[0],
        'initiate_time': get_current_timestamp()
    }

def get_distance(location1, location2):
    return location1.distance(location2)

def location_to_string(location):
    return '(%f, %f)' % (location.x, location.y)

def get_carla_vehicle_info(vehicle_id, carla_vehicle):

    return {
        "vehicle_id": vehicle_id, 
        "carla_id": carla_vehicle.id,
        "carla_type_id": carla_vehicle.type_id,
        "attributes": carla_vehicle.attributes,
        "location": location_to_string(carla_vehicle.get_location())
    }

def get_remaining_waypoint_count(agent):
    return len(agent.get_local_planner()._waypoints_queue)



class CollisionSensor(object):
    """ Class for collision sensors"""

    def __init__(self, parent_actor, notify):
        """Constructor method"""
        self.sensor = None
        self.history = []
        self._parent = parent_actor
        self.notify = notify
        world = self._parent.get_world()
        blueprint = world.get_blueprint_library().find('sensor.other.collision')
        self.sensor = world.spawn_actor(blueprint, carla.Transform(), attach_to=self._parent)
        # We need to pass the lambda a weak reference to
        # self to avoid circular reference.
        weak_self = weakref.ref(self)
        self.sensor.listen(lambda event: CollisionSensor._on_collision(weak_self, event))

    def get_collision_history(self):
        """Gets the history of collisions"""
        history = collections.defaultdict(lambda : (0, ''))
        for frame, intensity, type_id in self.history:
            history[frame] = (history[frame][0] + intensity, type_id)
        return history

    @staticmethod
    def _on_collision(weak_self, event):
        """On collision method"""
        self = weak_self()
        if not self:
            return
        actor_type = event.other_actor.type_id
        self.notify('Collision with %r' % actor_type)
        impulse = event.normal_impulse
        intensity = math.sqrt(impulse.x ** 2 + impulse.y ** 2 + impulse.z ** 2)
        self.history.append((event.frame, intensity, event.other_actor.type_id))
        if len(self.history) > 4000:
            self.history.pop(0)

    def destroy(self):
        self.sensor.destroy()

def waypoint_count_to_eta(waypoint_count):
    return 5*waypoint_count