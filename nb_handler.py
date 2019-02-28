#!/usr/bin/env python3
"""RPCS Northbound Block Handler"""
import asyncio
import binascii
import json
import socket
import struct
import pickle
import time
import traceback

from config import *
import misc
from states import RPCS


class NBCommands:
    #  Commands:
    # A - Get value
    nb_simple_commands = 'A'


class UnsupportedCommandException(Exception):
    """ Exception raised when the request received from the NB block
        is not yet implemented or completely not supported by RPCS
    """

    def __init__(self, cmd_code, cmd_opt=None):
        self.cmd_code = cmd_code
        self.cmd_opt = cmd_opt

    def __str__(self):
        if self.cmd_opt is None:
            return "NB Block: Unsupported command code: %s" % self.cmd_code
        else:
            return "NB Block: Option %s is not supported by command code %s" \
                   % (self.cmd_opt, self.cmd_code)


class InvalidResponseException(Exception):
    """ Exception raised when an error occurs during parsing, validation
        or translation of the received response to a given command
    """

    def __init__(self, command, response, reason):

        LOG.debug("NB Block: Could not parse response to Command (%s)" %
                  command)
        LOG.debug("Response: %s, reason: %s" % (response, reason))


class Command:

    """ General class for the Commands;
        Stores code, option and final result of the response handling
    """
    command_code = None
    command_option = None
    params = []
    result = 0

    def __str__(self):
        if self.command_option is not None:
            option = self.command_option
        else:
            option = ''

        return str(self.command_code) + str(option)

    def __init__(self, cmd_name=None, cmd_opt=None):
        self.command_code = cmd_name
        self.command_option = cmd_opt

    def get_command_tuple(self):
        return self.command_code, self.command_option

    def instantiate_command(self):

        # NB Command list (example)
        command_mapping = {
            'A': GetValue,
        }

        command = command_mapping[self.command_code]
        LOG.debug("Command %s instantiated" % command.__name__)
        return command(self.command_code, self.command_option)

    def parse_nb_request(self, nb_request):

        """ Extract command (and option if available) from NB request message
            and return it in the form of a Command derived class object
        """
        command = None
        self.command_code = nb_request[1]
        LOG.debug("NB Block: Northbound Request: %s" % self.command_code)

        if self.command_code in NBCommands.nb_simple_commands:
            command = self.instantiate_command()
        else:
            LOG.warning("NB Block:Unsupported command: %s" %
                        self.command_code)
            raise UnsupportedCommandException(self.command_code)

        return command

    def pack_response(self, response):
        self.result = 1
        response = struct.pack('<B', response.pop())

        return self.result, response


class GetValue(Command):

    value_params = {
        # Get all
        0: [],
        # Single Element
        1: ['Device.Element.1'],
    }

    def __init__(self, cmd_name=None, cmd_opt=None):
        self.rModel = Model()
        self.params = self.value_params.get(cmd_opt)

        super().__init__(cmd_name, cmd_opt)

    def translate_response(self, response):
        response_map = {}

        for key, value in response.items():
            self.rModel.add(key, value)

        try:
            if self.command_option in [0, 1]:  # get all or single element
                element = self.rModel.get_from_model(
                    'Device.Element.1', mandatory=True)
                # Validate response: element must be one unsigned int (example)
                if (not isinstance(element, int) or
                        not (0 <= uptime <= 4294967295)):
                    # Unexpected uptime
                    LOG.debug("NB Block: Invalid response to Get Uptime")
                    raise ValueError
                else:
                    response_map['element'] = element
        except (struct.error, ValueError, TypeError, KeyError):
            raise InvalidResponseException(str(self), response,
                                              'Invalid response')
        else:
            return response_map

    def pack_response(self, response):
        # Get value (G)
        # Option: Which attribute value to request
        # Special response format: <result>:<cmdcode>:<option><result>
        response = None

        if response == 'error' or None:
            return self.result, response

        # At this point we can expect valid response values
        self.result = 1

        # copy option-id from request and add response
        # as unsigned int value (32 bit, little endian)
        response = struct.pack('<b', int(self.command_option))
        if self.command_option == 2:
            response += struct.pack('<I', int(response['element']))

        return self.result, response


def get_client_queue(client_id):
    """ If there is already a list for this client in the queue,
        return it. Otherwise, create list first and return it.
    """
    if client_id in RPCS.Northbound_Queue:
        return RPCS.Northbound_Queue[client_id]
    else:
        RPCS.Northbound_Queue[client_id] = []
        return RPCS.Northbound_Queue[client_id]


def remove_from_client_queue(client_id, command_socket_tuple):

    """ If command is in client's queue, remove it.
        If client queue is empty, remove it.
    """

    if command_socket_tuple in RPCS.Northbound_Queue[client_id]:
        RPCS.Northbound_Queue[client_id].remove(command_socket_tuple)
        # Check if client entry can be removed form Northbound Queue
        if not RPCS.Northbound_Queue[client_id]:
            RPCS.Northbound_Queue.pop(client_id)


def send_connection_request(ip):

    # Generate UDP packet with new identifier
    RPCS.Northbound_Queue['udp_id'] += 1
    payload = str(RPCS.Northbound_Queue['udp_id']) + ':CONNECTION_REQUEST'

    # Send UDP packet 3 times with some pause in between to increase
    # the chance it arrives at the as UDP is 'unreliable' by nature
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.sendto(payload.encode(), (ip, RPCSD_PORT))
        time.sleep(0.3)
        sock.sendto(payload.encode(), (ip, RPCSD_PORT))
        time.sleep(0.3)
        sock.sendto(payload.encode(), (ip, RPCSD_PORT))
    except OSError:
        LOG.warning("NB Block: Failed to send Connection Request")
    finally:
        sock.close()


def create_nb_response(response, command):

    """ Check response from and return NB response message;
        Call the specific class of each Command's pack_response method,
        which will return if it was successful in packing the response or not,
        together with the packed response, ready to be sent back to the NB
        block
    """
    response = ''
    result = 0

    if response is not None and response != 'error':
        # Got data, we are done: respond with success code and result
        result, response = command.pack_response(response)
    else:
        LOG.debug("NB Block: Could not finalize NB request")

    message = str(result).encode()
    message += b':' + command.command_code.encode()

    if response:
        # result:cmdcode:hex-response\n
        message += b':' + binascii.hexlify(response) + b'\n'
    else:
        # result:cmdcode\n
        message += b'\n'

    return message


@asyncio.coroutine
def nb_session_handler(nb_reader, nb_writer):

    """ This is a call back routine for a Northbound session
        Read messages from the NB interface, send connection request
        wait for result to be returned and write it back.

        Request message:

        The request messages starts with the clientid followed by a single
        character as the command code (cmdcode) and, optionally, one or more
        bytes in hexadecimal notation for arguments (options). The fields are
        separated by a colon. The message is terminated with a line-feed.

        Syntax: "<clientid>:<cmdcode>[:<options>]\n"

        Example: "my_client:G:01\n" - command "Get Element" for client "my_cpe"

        Response message:

        The response message starts with the result integer (1 for success,
        0 for error), followed by the command code (cmdcode) and, optionally,
        the remainder of the response payload in hexadecimal notation. The
        fields are separated by a colon. The message is terminated with a
        line-feed.

        Syntax: "<result>:<cmdcode>[:<response>]\n"

        Example: "1:G:0300\n" - successfully processed command "Get Element",
                                return value 0x0300
        Example: "0:G\n" - error processing request
    """
    response = None
    command = Command()
    command_socket_tuple = ()
    loop = asyncio.get_event_loop()
    db_session = {}

    # Get request from the Northbound interface
    nb_request = yield from nb_reader.readline()

    # Convert to string, remove newline, and split fields
    decoded_request = nb_request.decode().rstrip().split(':')

    # Obtain client_id from request
    client_id = decoded_request[0]

    try:
        # Parse and validate the request
        command = command.parse_nb_request(decoded_request)

        # Obtain ip address from database based on the client_id
        RPCS.Authenticating.connect_database(db_session)
        row = db_session['db'].simple_query(Queries.IP, (client_id, client_id, client_id, client_id))
        ip = row[0].decode()

        # Add client to queue
        client_queue = get_client_queue(client_id)

        # Create pair of local sockets to communicate with session handler
        # The Response stream socket will then be used to receive the response from the cpe.
        (response_rsock, response_wsock) = socket.socketpair()
        (reader, writer) = yield from asyncio.open_connection(sock=response_rsock, loop=loop)

        # Create a tuple in the form '((client_COMMAND), RESPONSE_STREAM)' and
        #                              |___tuple___|, |_local_socket_|
        # and add it to the client queue to be processed
        command_socket_tuple = (command, response_wsock)
        client_queue.append(command_socket_tuple)

        # Send the Connection Request to the client
        send_connection_request(ip)

        # Read out all the response from session, timeout 20 seconds
        result = yield from asyncio.wait_for(reader.read(40000), timeout=RPCSD_TIMEOUT)
        response = pickle.loads(result)
        if response != 'error' and response is not None:
            # Create and send response message via Northbound interface
            response = command.translate_response(response)

    except UnsupportedCommandException as err:
        LOG.debug(str(err))
    except asyncio.TimeoutError:
        LOG.debug("NB Block: Connection request timeout")
    except pickle.UnpicklingError:
        LOG.debug("NB Block: No valid response from client")
        response = 'error'
    except DbException as err:
        LOG.debug("NB Block: Could not find client's IP address")
        LOG.debug("{!s}".format(err))
    except InvalidResponseException as err:
        response = 'error'
        LOG.debug("NB Block: Unexpected error while parsing "
                  "response: {!s}".format(err))
    except Exception as err:
        traceback.print_exc()
        response = 'error'
        LOG.debug("NB Block: Unexpected error in NB session %s" % err)
    finally:
        nb_response = create_nb_response(response, command)
        nb_writer.write(nb_response)
        nb_writer.close()
        # Remove command from client queue
        if 'client_queue' in locals():
            remove_from_client_queue(client_id, command_socket_tuple)
        # Close sockets and disconnect database session
        if 'writer' in locals():
            writer.close()
        if 'response_rsock' in locals():
            response_rsock.shutdown(socket.SHUT_RD)
            response_rsock.close()
        if 'response_wsock' in locals():
            response_wsock.shutdown(socket.SHUT_WR)
            response_wsock.close()
        if 'db' in db_session:
            db_session['db'].disconnect()
        LOG.debug("NB Block: Response sent back to NB block: %s" %
                  str(nb_response))
