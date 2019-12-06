import struct
import os
import time


class ClientManager:
    clients = []

    def __init__(self):
        self.clients = []

    def create_client(self, client_name, number_of_workers, update_interval, exponent, iterations):
        client = Client(client_name, number_of_workers, update_interval)
        client.add_assignment(exponent, iterations)
        self.clients.append(client)

    def add_or_update_client(self, client_name, number_of_workers, update_interval, exponent, iterations):
        for i in range(0, len(self.clients)):
            if self.clients[i].name == client_name:
                self.update_client_attributes(client_name, number_of_workers, update_interval)
                self.add_or_update_client_assignment(client_name, exponent, iterations)
                return

        self.create_client(client_name, number_of_workers, update_interval, exponent, iterations)

    def update_client_attributes(self, client_name, number_of_workers, update_interval):
        for i in range(0, len(self.clients)):
            if self.clients[i].name == client_name:
                self.clients[i].update_attributes(number_of_workers, update_interval)

    def add_or_update_client_assignment(self, client_name, exponent, iterations):
        for i in range(0, len(self.clients)):
            if self.clients[i].name == client_name:
                if self.clients[i].check_assignment(exponent):
                    self.clients[i].update_assignment(exponent, iterations)
                    return
                else:
                    self.clients[i].add_assignment(exponent, iterations)
                    return

    def get_assignments_sorted(self):
        assignments = []

        for i in range(0, len(self.clients)):
            for j in range(0, len(self.clients[i].assignments)):
                assignments.append(self.clients[i].assignments[j])

        return sorted(assignments, key=lambda x: x.progress, reverse=True)


class Client:
    name = ''
    number_of_workers = 0
    update_interval = 0
    assignments = []

    def __init__(self, name, number_of_workers, update_interval):
        self.name = name
        self.update_attributes(number_of_workers, update_interval)
        self.assignments = []

    def update_attributes(self, number_of_workers, update_interval):
        self.number_of_workers = int(number_of_workers)
        self.update_interval = int(update_interval)

    def check_assignment(self, exponent):
        for assignment in self.assignments:
            if assignment.exponent == exponent:
                return True
        return False

    def clean_assignments(self):
        '''
        There can be instances where the server isn't notified of an assignment removal. This will result in the client
        having more assignments allocated to it than it should. We can remedy this by looping through all the
        assignments, and removing the oldest one's until the number of assignments matches the number of workers for the
        server.
        '''

        while len(self.assignments) > self.number_of_workers:
            oldest_assignment_last_updated_time = int(time.time() + 86400)
            oldest_assignment_index = 0

            # Identify the oldest assignment.
            for i in range(0, len(self.assignments)):
                if self.assignments[i].last_updated < oldest_assignment_last_updated_time:
                    oldest_assignment_last_updated_time = self.assignments[i].last_updated
                    oldest_assignment_index = i

            # Remove the oldest assignment
            self.assignments.pop(oldest_assignment_index)

    def add_assignment(self, exponent, iterations):
        new_assignment = Assignment(self.name, exponent, iterations, self.update_interval)
        self.assignments.append(new_assignment)

        # Make sure we don't have more assignments than we have workers for this client.
        self.clean_assignments()

    def update_assignment(self, exponent, iterations):
        for assignment in self.assignments:
            if assignment.exponent == exponent:
                assignment.update_iterations(iterations)
                return

    def remove_assignment(self, exponent):
        for index in range(0, len(self.assignments)):
            if self.assignments[index].exponent == exponent:
                self.assignments.pop(index)
                return

    def get_number_of_assignments(self):
        return int(len(self.assignments))


class Assignment:
    client_name = ""
    exponent = 0
    exponent_digit_length = 0
    iterations = 0
    progress = 0.0
    last_updated = 0
    update_interval = 0
    estimated_completion_date = 0
    average_iterations_per_second = []

    def __init__(self, client_name, exponent, iterations, update_interval):
        self.client_name = client_name
        self.exponent = int(exponent)
        self.exponent_digit_length = int(round(exponent * 0.301029995664))
        self.update_interval = int(update_interval)
        self.last_updated = int(time.time())
        self.update_iterations(iterations)
        self.average_iterations_per_second = []

    def update_iterations(self, new_iterations):
        if self.iterations > 0:
            iterations_since_last_update = int(new_iterations) - self.iterations
        else:
            iterations_since_last_update = 0

        if iterations_since_last_update > 0:
            self.average_iterations_per_second.append(iterations_since_last_update / (self.update_interval * 1.0))

        self.iterations = int(new_iterations)

        if len(self.average_iterations_per_second) > (21600 / self.update_interval):
            self.average_iterations_per_second.pop(0)

        self.last_updated = int(time.time())
        self.update_estimated_completion_date()
        self.update_progress()

    def get_average_iterations_per_second(self):
        average_iterations_per_second = 0

        for iterations in self.average_iterations_per_second:
            average_iterations_per_second = average_iterations_per_second + iterations

        if len(self.average_iterations_per_second) == 0:
            return 0.0
        else:
            return average_iterations_per_second / (len(self.average_iterations_per_second) * 1.0)

    def update_estimated_completion_date(self):
        average_iterations_per_second = self.get_average_iterations_per_second()

        if average_iterations_per_second == 0:
            self.estimated_completion_date = int(0)
        else:
            self.estimated_completion_date = int(time.time() + ((self.exponent - self.iterations) /
                                                                average_iterations_per_second))

    def update_progress(self):
        self.progress = round(100 * (self.iterations * 1.0) / (self.exponent * 1.0), 2)


class AssignmentFile:
    file_path = ''
    exponent = 0
    iterations = 0
    previous_iterations = 0
    last_modified = 0
    time_to_check = 0
    time_to_sleep = 0

    def __init__(self, file_path):
        self.file_path = file_path
        self.read_file()

    def read_file(self):
        if not os.path.isfile(self.file_path):
            raise Exception(self.file_path + " does not exist.")

        self.last_modified = int(os.path.getmtime(self.file_path))

        with open(self.file_path, 'rb') as data_file:
            byte_stream = data_file.read(60)

            data_file.close()

            self.exponent = int(struct.unpack_from("<L", byte_stream, 20)[0])
            self.previous_iterations = self.iterations
            self.iterations = int(struct.unpack_from("<L", byte_stream, 56)[0])
