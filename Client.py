from random import randint
from Modules import Gimps
import json
import logging
import os
import glob
import time
import socket
import signal
import sys
import platform


class Config:
    name = ''
    workers = 0
    data_directory = ''
    data_update_interval = 0
    server_ip = ''
    server_port = 0

    def __init__(self):
        with open('client.config.json', 'r') as fp:
            obj = json.load(fp)
            fp.close()

        self.name = obj['client']['name']
        self.workers = int(obj['client']['workers'])
        self.data_directory = obj['client']['data_directory']
        self.data_update_interval = int(obj['client']['data_update_interval'])

        self.server_ip = obj['server']['ip']
        self.server_port = int(obj['server']['port'])


# Catch SIGINT
def signal_handler(sig, frame):
    print "Exiting Program..."
    sys.exit(0)


# Listen on Signal Events
signal.signal(signal.SIGINT, signal_handler)

# Global Variables
ASSIGNMENTS = []
TIME_TO_WAIT = 0
ASSIGNMENT_TO_CHECK = 0
DIRECTIVE = "INSTANTIATE"  # VALID VALUES: INSTANTIATE, LOCATE, MONITOR
CONFIG = Config()

# Logging Parameters
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def locate_new_assignment_files():
    # Change directory to the directory specified in the config.
    os.chdir(CONFIG.data_directory)

    # Loop through each file in the data directory.
    for directory_file in glob.glob("p*"):

        # Determine which divider we're using.
        divider = "\\"

        if platform.system() == "Linux":
            divider = "/"

        # Generate the full path of the file.
        full_directory_file_path = CONFIG.data_directory + divider + directory_file

        # Verify the file doesn't have a dot (.) and that the file isn't already being monitored.
        if '.' not in directory_file and not any(assignment_file.file_path == full_directory_file_path
                                                 for assignment_file in ASSIGNMENTS):

            # Create a new assignment and add it to the ASSIGNMENTS list.
            try:
                ASSIGNMENTS.append(Gimps.AssignmentFile(full_directory_file_path))
            except:
                logging.error("Attempted to add assignment file to list that doesn't exist. File: " +
                              full_directory_file_path)


def calculate_time_to_wait():
    # Specify Global Variables
    global TIME_TO_WAIT
    global ASSIGNMENT_TO_CHECK

    # Create containers.
    oldest_assignment_last_modified_time = int(time.time() + 86400)
    oldest_assignment_index = 0

    # Identify the oldest assignment. The oldest assignment should be the next assignment the program checks.
    for i in range(0, len(ASSIGNMENTS)):
        if ASSIGNMENTS[i].last_modified < oldest_assignment_last_modified_time:
            oldest_assignment_last_modified_time = ASSIGNMENTS[i].last_modified
            oldest_assignment_index = i

    # Set the global ASSIGNMENT_TO_CHECK variable.
    ASSIGNMENT_TO_CHECK = oldest_assignment_index

    # Calculate how old the oldest assignment is in seconds.
    oldest_assignment_age = int(time.time() - oldest_assignment_last_modified_time)

    '''
    If the oldest assignment is older than the update interval, and the last time we updated it there was a difference
    between the iterations, then set TIME_TO_WAIT to 1. If the oldest assignment is older than the update interval, but
    last time we updated the iterations were the same, then set TIME_TO_WAIT to the data_update interval. Otherwise,
    determine how much time is left before we need to update the oldest assignment, and set TIME_TO_WAIT to that value
    plus a random number between 1 and 5.
    '''
    if oldest_assignment_age > CONFIG.data_update_interval \
            and (ASSIGNMENTS[oldest_assignment_index].previous_iterations
                 != ASSIGNMENTS[oldest_assignment_index].iterations):
        TIME_TO_WAIT = 1
        logging.debug("calculate_time_to_wait()| Assignment is older than update interval, and not duplicate.")
    elif oldest_assignment_age > Config.data_update_interval \
            and (ASSIGNMENTS[oldest_assignment_index].previous_iterations
                 == ASSIGNMENTS[oldest_assignment_index].iterations):
        TIME_TO_WAIT = CONFIG.data_update_interval
        logging.debug("calculate_time_to_wait()| Assignment is older than update interval, and duplicate.")
    else:
        TIME_TO_WAIT = CONFIG.data_update_interval - oldest_assignment_age + randint(1, 5)
        logging.debug("calculate_time_to_wait()| Assignment is not older than update interval")


def do_server_communication(exponent, iterations):
    # Create Server Message
    """
    Message Format
    CLIENT_NAME|CLIENT_WORKERS|UPDATE_INTERVAL|ASSIGNMENT_EXPONENT|ASSIGNMENT_ITERATIONS
    """
    server_message = CONFIG.name + "|" + str(CONFIG.workers) + "|" + str(CONFIG.data_update_interval) + "|" + str(
        exponent) + "|" + str(iterations)

    # Create Socket Connection
    socket_connection = socket.socket()

    try:
        # Connect to Server
        socket_connection.connect((CONFIG.server_ip, CONFIG.server_port))
        logging.debug("do_server_communication()| Connected to " + str(CONFIG.server_ip) + ":"
                      + str(CONFIG.server_port) + ".")

        # Send Message to Server
        socket_connection.send(server_message)
        logging.debug("do_server_communication()| Client to Server Message: " + server_message)

        # Disconnect from Server
        socket_connection.close()
        logging.debug("do_server_communication()| Disconnected from " + str(CONFIG.server_ip) + ":"
                      + str(CONFIG.server_port) + ".")
    except:
        # Unable to connect to server, so let's log this.
        logging.error("do_server_communication()| Unable to connect to " + str(CONFIG.server_ip) + ":"
                      + str(CONFIG.server_port) + ".")


while True:
    logging.debug("Sleeping for " + str(TIME_TO_WAIT) + " seconds.")
    time.sleep(TIME_TO_WAIT)

    if DIRECTIVE == "INSTANTIATE":
        logging.debug("DIRECTIVE is INSTANTIATE.")

        # Identify and read all data files.
        locate_new_assignment_files()
        logging.debug("Located all new assignment files.")

        # Send all data file information to the server.
        for assignment in ASSIGNMENTS:
            do_server_communication(assignment.exponent, assignment.iterations)
        logging.debug("Notified server of all data files.")

        # Determine how long the program should sleep.
        calculate_time_to_wait()
        logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

        # Set DIRECTIVE to MONITOR.
        DIRECTIVE = "MONITOR"
        logging.debug("Set DIRECTIVE to MONITOR.")

        # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
        logging.debug("Resetting Loop.")
        continue

    elif DIRECTIVE == "MONITOR":
        logging.debug("DIRECTIVE is MONITOR.")

        try:
            # Read the assignment file from disk, and update values.
            ASSIGNMENTS[ASSIGNMENT_TO_CHECK].read_file()
            logging.debug("Updated assignment " + str(ASSIGNMENT_TO_CHECK) + "'s data.")

            # Notify server of changes.
            do_server_communication(ASSIGNMENTS[ASSIGNMENT_TO_CHECK].exponent,
                                    ASSIGNMENTS[ASSIGNMENT_TO_CHECK].iterations)

            # Determine how long the program should sleep.
            calculate_time_to_wait()
            logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

            # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
            logging.debug("Resetting Loop.")
            continue

        except:
            # Notify server of changes.
            do_server_communication(ASSIGNMENTS[ASSIGNMENT_TO_CHECK].exponent, -1)

            # Remove assignment file from list.
            ASSIGNMENTS.pop(ASSIGNMENT_TO_CHECK)
            logging.debug("Removed assignment " + str(ASSIGNMENT_TO_CHECK) + " from list.")

            # Identify and read all assignment files.
            locate_new_assignment_files()
            logging.debug("Located all new assignment files.")

            # If the list is empty...
            if len(ASSIGNMENTS) == 0:
                logging.debug("List is empty.")

                # Set DIRECTIVE to LOCATE.
                DIRECTIVE = "LOCATE"
                logging.debug("Set DIRECTIVE to LOCATE.")

                # Set TIME_TO_WAIT to 60.
                TIME_TO_WAIT = 60
                logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
                logging.debug("Resetting Loop.")
                continue

            else:
                logging.debug("List is not empty.")

                # Determine how long the program should sleep.
                calculate_time_to_wait()

                # Verify that the client is aware of the correct number of assignments.
                if len(ASSIGNMENTS) < CONFIG.workers:
                    logging.debug("List contains fewer assignments than workers for this client.")

                    # Wait a maximum of 60 seconds before checking for the new assignments.
                    if TIME_TO_WAIT > 60:
                        TIME_TO_WAIT = 60
                    logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                    # Set DIRECTIVE to LOCATE.
                    DIRECTIVE = "LOCATE"
                    logging.debug("Set DIRECTIVE to LOCATE.")

                    # Continue WHILE Loop, which will sleep the program for the specified amount of time in
                    # TIME_TO_WAIT.
                    logging.debug("Resetting Loop.")
                    continue

                else:
                    logging.debug("List contains the exact number of assignments per worker for this client.")

                    logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                    # Continue WHILE Loop, which will sleep the program for the specified amount of time in
                    # TIME_TO_WAIT.
                    logging.debug("Resetting Loop.")
                    continue

    elif DIRECTIVE == "LOCATE":
        logging.debug("DIRECTIVE is LOCATE.")

        # If the list is empty...
        if len(ASSIGNMENTS) == 0:
            logging.debug("List is empty.")

            # Identify and read all assignment files.
            locate_new_assignment_files()
            logging.debug("Located all new assignment files.")

            # If the list is still empty...
            if len(ASSIGNMENTS) == 0:
                logging.debug("List is still empty.")

                # Set TIME_TO_WAIT to 60.
                TIME_TO_WAIT = 60
                logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
                logging.debug("Resetting Loop.")
                continue
            else:
                logging.debug("List is no longer empty.")

                # Set TIME_TO_WAIT to 1.
                TIME_TO_WAIT = 1
                logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
                logging.debug("Resetting Loop.")
                continue
        else:
            logging.debug("List is not empty.")

            # Identify and read all assignment files.
            locate_new_assignment_files()
            logging.debug("Located all new assignment files.")

            # Determine how long the program should sleep.
            calculate_time_to_wait()
            logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

            if len(ASSIGNMENTS) < CONFIG.workers:
                logging.debug("List contains fewer assignments than workers for this client.")

                # Wait a maximum of 60 seconds before checking for the new assignments.
                if TIME_TO_WAIT > 60:
                    TIME_TO_WAIT = 60
                logging.debug("Set TIME_TO_WAIT to " + str(TIME_TO_WAIT) + ".")

                # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
                logging.debug("Resetting Loop.")
                continue

            else:
                logging.debug("List contains the exact number of assignments per worker for this client.")

                # Set DIRECTIVE to MONITOR.
                DIRECTIVE = "MONITOR"
                logging.debug("Set DIRECTIVE to MONITOR.")

                # Continue WHILE Loop, which will sleep the program for the specified amount of time in TIME_TO_WAIT.
                logging.debug("Resetting Loop.")
                continue
