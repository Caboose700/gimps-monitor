import time
import os
import socket
import signal
import json
import ftplib
from datetime import datetime
from tabulate import tabulate
from Modules import Gimps


class Config:
    host = ''
    port = 0

    date_format = ''
    clear_command = ''
    table_type = ''
    print_to_file = False
    print_to_file_name = ''
    print_to_file_ftp = False

    ftp_host = ''
    ftp_user = ''
    ftp_pass = ''
    ftp_path = ''

    def __init__(self):
        with open('server.config.json', 'r') as fp:
            obj = json.load(fp)
            fp.close()

        self.host = obj['server']['host']
        self.port = int(obj['server']['port'])

        self.date_format = obj['display']['date_format']
        self.table_type = obj['display']['table_type']
        self.print_to_file = bool(obj['display']['print_to_file'])
        self.print_to_file_name = obj['display']['print_to_file_name']
        self.print_to_file_ftp = bool(obj['display']['print_to_file_ftp'])

        self.ftp_host = obj['ftp']['host']
        self.ftp_user = obj['ftp']['user']
        self.ftp_pass = obj['ftp']['pass']
        self.ftp_path = obj['ftp']['pass']

        if obj['display']['clear_command'] == 'cls' or obj['display']['clear_command'] == 'clear':
            self.clear_command = obj['display']['clear_command']


CLIENT_MANAGER = Gimps.ClientManager()
CONFIG = Config()


# Catch SIGINT
def signal_handler(sig, frame):
    print "Exiting Program..."
    os.sys.exit(0)


# Listen on Signal Events
signal.signal(signal.SIGINT, signal_handler)


def display_output():

    # Get sorted assignments from client manager.
    assignments = CLIENT_MANAGER.get_assignments_sorted()

    # Create table data.
    table_headers = ['Client', 'Exponent', 'Progress', 'Avg. Iterations', 'Est. Completion', 'Last Updated']
    table_data = []

    # Loop through each assignment, and add its information to the list.
    for assignment in assignments:
        estimated_completion_date_string = 'To Be Determined'
        average_iterations_per_second_string = 'To Be Determined'

        if assignment.estimated_completion_date != 0:
            estimated_completion_date_string = datetime.fromtimestamp(assignment.estimated_completion_date).strftime(
                CONFIG.date_format)

        if assignment.average_iterations_per_second != 0:
            average_iterations_per_second_string = '{:.2f} iters/sec'.format(
                round(assignment.get_average_iterations_per_second(), 2))

        progress_string = '{}% ({:,} iterations)'.format(assignment.progress, assignment.iterations)

        table_data.append([
            assignment.client_name,
            '{:,} ({:,} digits)'.format(assignment.exponent, assignment.exponent_digit_length),
            progress_string,
            average_iterations_per_second_string,
            estimated_completion_date_string,
            datetime.fromtimestamp(assignment.last_updated).strftime(CONFIG.date_format)
        ])

    # Print table to screen.
    os.system(CONFIG.clear_command)
    print tabulate(table_data, table_headers, CONFIG.table_type, stralign='center')

    # (Optionally) Print table to file.
    if CONFIG.print_to_file and CONFIG.print_to_file_name != '':
        # Write data to file.
        with open(CONFIG.print_to_file_name, 'w') as fp:
            fp.write(tabulate(table_data, table_headers, CONFIG.table_type, stralign='center'))
            fp.close()

        # (Optionally) Upload data file to ftp.
        if CONFIG.print_to_file_ftp:
            try:
                ftp_session = ftplib.FTP(CONFIG.ftp_host, CONFIG.ftp_user, CONFIG.ftp_pass)
                table_file = open(CONFIG.print_to_file_name, 'rb')
                ftp_session.storbinary("STOR " + CONFIG.print_to_file_name, table_file)
                table_file.close()
                ftp_session.close()
            except Exception:
                time.sleep(0)


def parse_client_message(message):
    # Parse the message received from the client.
    '''
    Message Format
    CLIENT_NAME|CLIENT_WORKERS|UPDATE_INTERVAL|ASSIGNMENT_EXPONENT|ASSIGNMENT_ITERATIONS
    '''
    split_message = message.split("|")

    # Retrieve values from client message.
    client_name = str(split_message[0])
    client_number_of_workers = int(split_message[1])
    client_update_interval = int(split_message[2])
    client_assignment_exponent = int(split_message[3])
    client_assignment_iterations = int(split_message[4])

    # Add or update client in client manager.
    CLIENT_MANAGER.add_or_update_client(client_name, client_number_of_workers, client_update_interval,
                                        client_assignment_exponent, client_assignment_iterations)


# Display table.
display_output()

# Create socket connection to listen for clients.
socket_connection = socket.socket()
socket_connection.bind((CONFIG.host, CONFIG.port))
socket_connection.listen(10)

while True:
    c, addr = socket_connection.accept()
    client_message = c.recv(1024)
    c.close()

    parse_client_message(client_message)
    display_output()
