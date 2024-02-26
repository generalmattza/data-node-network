from data_node_network.data_gatherer import GathererNodeTCP


class Sensor:
    def __init__(self, name, sensor_type):
        self.name = name
        self.sensor_type = sensor_type

    def read(self):
        raise NotImplementedError("Subclasses must implement read method")


class SensorNode(GathererNodeTCP):
    def __init__(
        self, sensors: list[Sensor], host="localhost", port=0, name=None, node_id=None
    ):
        super().__init__(host=host, port=port, name=name, node_id=node_id)
        self.sensors = sensors

    def gather_data(self):
        data = [sensor.read() for sensor in self.sensors]
        return data

    def process_data(self, data):
        return data
