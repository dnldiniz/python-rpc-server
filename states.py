#!/usr/bin/env python3
"""RPCS NG automatic communication server"""
import asyncio
import re
import random
import ssl
import traceback

from distutils.version import StrictVersion

from config import LOG, RPCS_INTERFACE, RPCS_PORT
from db import Db, DbException
from misc import get_element, MandatoryFieldNotFoundError, UnexpectedFieldDataType
from rpc import ClientMethodException, RpcExecute


class ClientResponseError(Exception):
    """
        Exception raised when client returns an 'error' as response to a RPC
        request
    """

    def __init__(self, message, reason, data=''):

        if reason == 'wrong_rpc_id':
            self.log = ("Parsing of response from client returned error "
                        "parameter RPC ID in response does not match expected id: %s", data)

        if reason == 'error':
            error = get_element('error', message)
            # Error during response parsing, log and move on
            code = get_element('code', error)
            message = get_element('message', error)
            data = get_element('data', error)
            data = ('' if data is None else ", data='" + data + "'")

            self.log = ("Parsing of response from client returned error (%s: '%s'%s)" % (code, message, data))

    def __str__(self):
        return repr(self.log)


class ClientRequestError(Exception):
    """
        Exception raised when something goes wrong during validation
        of RPC parameters
    """

    def __init__(self, error, data='', critical=False):

        error_definition = {
            'MandatoryFieldNotFoundError': {
                'error': {
                    'code': -31992,
                    'message': 'Missing RPC parameter',
                    'data': data
                },
                'log': 'Mandatory parameter not found',
                'next_state': RPCS.ExpectInform,
            },
            'UnexpectedFieldDataType': {
                'error': {
                    'code': -31993,
                    'message': 'Invalid RPC parameter value',
                    'data': data
                },
                'log': 'Unexpected data type for inform parameter',
                'next_state': (RPCS.ExpectInform if critical is True else
                               RPCS.ExpectRpc)
            },
            'InvalidParameterValue': {
                'error': {
                    'code': -31993,
                    'message': 'Invalid RPC parameter value',
                    'data': data
                },
                'log': 'Unexpected value for inform parameter',
                'next_state': (RPCS.ExpectInform if critical is True else
                               RPCS.ExpectRpc)
            },
            'InvalidParameterName': {
                'error': {
                    'code': -31994,
                    'message': 'Invalid RPC parameter name',
                    'data': data
                },
                'log': 'Unexpected parameter in inform message',
                'next_state': RPCS.ExpectRpc
            },
            'NoValidProtocolVersionInCommon': {
                'error': {
                    'code': -31996,
                    'message': 'No protocol version in common',
                },
                'log': "Client has no protocol in common with RPCS Server",
                'next_state': RPCS.ExpectInform
            },
            'UnknownClient': {
                'error': {
                    'code': -31999,
                    'message': 'Unknown CLIENT ',
                },
                'log': 'Could not validate client in the database',
                'next_state': RPCS.Listening
            }
        }

        self.error_name = error
        self.error = error_definition[error]
        self.data = data

    def __str__(self):

        return repr("Error during RPC validation: %s Data: %s" %
                    (self.error['log'], self.data))


class SendingRpc:

    """ State: Send RPCs to the CLIENT

        Check session queue from RPCs to be sent to the client
        If no RPCs in queue, send 'done'. Otherwise, send RPC and move to state
        ExpectResponse
    """

    def add_nb_queue_to_session_queue(self, session):

        """ Check the northbound queue for RPC's queued by GUI or SOAP
            requests. A client should connect triggered by a CONNECTION_REQUEST
            and any RPC's queued by the northbound will be then added to the
            session queue by this function.

            Input parameters:
                session       - Used to assign values for future use
        """
        rpc_list = []
        client_id = get_element('cid', session['client'])

        if client_id is not None and client_id in RPCS.Northbound_Queue:
            # Check if all commands have been serviced
            if RPCS.Northbound_Queue[client_id]:
                # Get first request in the client queue, in the form:
                #   (Client_COMMAND, RESPONSE STREAM)
                # TODO pop might be unresolved
                nb_request = RPCS.Northbound_Queue[client_id].pop(0)
                # Parse and queue request(s)
                client_command = nb_request[0]
                rpc_list.append(client_command)
                # Insert nb commands to the front of queue
                session['queue'] = queued_nb_methods + session['queue']
                # Store stream which expects the client response in the session
                session['nb_response_stream'] = nb_request[1]

    def run(self, session):
        """ When NB requests are queued put them in the session queue.

            If there are RPCs in queue to be sent, return prepared RPC
            and move to ExpectResponse. Otherwise, go to Listening and
            send a 'done' RPC.

            Input parameters:
                session       - Used to assign values for future use

            Return values:
                next_state    - Next state of the state machine: Either
                                Listening or ExpectResponse
                rpc           - RPC to be sent to the CLIENT
            Raises:
                ClientMethodException - When an error occurs during
                                RPC preparation
                DbException - When an database related error occurs
        """
        rpc = None
        if session['client']['event'] == 'CONNECTION_REQUEST':
            self.add_nb_queue_to_session_queue(session)

        while rpc is None and session['queue']:
            try:
                # Loop through queue until there is an RPC to send, or until
                # there are no more RPCs queued, or until an error occurs
                session['rpc']['method'] = session['queue'].pop(0)
                rpc = session['rpc']['method'].send_request(session)
            except ClientMethodException:
                # Failed to send this RPC, move on to the next
                LOG.debug("Error during preparation of client method: %s" % str(session['rpc']['method']))
                continue
            except Exception:
                traceback.print_exc()
                LOG.debug("Unexpected error during preparation of client method: %s" % str(session['rpc']['method']))
                return RPCS.SendingRpc, None

        if rpc is not None:
            # RPC ready: Send it and ExpectResponse
            return RPCS.ExpectResponse, rpc
        else:
            # If there are no (more) RPCs to send, log ok
            # and send done, indicating communication is complete
            session['log'] = {'rc': 'ok', 'msg': ''}
            session['db'].clear_dirtyflag(session['client']['cid'])
            return RPCS.Listening, {'method': 'done'}


class ExpectResponse:
    """ State: Expect response to previously sent RPC
               Handles response according to current defined action
    """

    def run(self, session):
        return self.handle_message(session, session['rpc']['message'])

    def handle_message(self, session, message):
        """ Handle a response to an RPC. Independently of the outcome,
            we always move back to SendingRpc state so that the other RPCs
            in queue can have a chance to be handled
        """
        rpc_id = get_element('id', message)

        try:
            if 'error' in session['rpc']['message']:
                raise ClientResponseError(session['rpc']['message'], 'error')
            if session['rpc']['id'] != rpc_id:
                # RPC id in response does not match request, log and move on
                data = str(session['rpc']['id']) + ' != ' + str(rpc_id)
                raise ClientResponseError(session['rpc']['message'], 'wrong_rpc_id', data=data)
            else:
                # Handle response for the RPC method that was last sent to CLIENT
                session['rpc']['method'].handle_response(session, message)
        except (ClientMethodException, ClientResponseError) as err:
                # Error parsing response, handle it and move on
                LOG.debug("Error parsing client response: {!s}".format(err))
                error_handler = getattr(session['rpc']['method'], "handle_error", None)

                if callable(error_handler):
                    # Call this client Method's specific error handler, if defined
                    error_handler(session)

        except (BrokenPipeError, Exception) as err:
            if isinstance(err, BrokenPipeError):
                # Silenced exception; client took too long to process and
                # respond to the Northbound request
                pass
            else:
                # Unexpected exception, log it with traceback and move on
                LOG.debug('Unexpected error during client Response handling: %s', str(err))
                LOG.debug(traceback.print_exc())

        return RPCS.SendingRpc, None


class ExpectRpc:

    """
        State: Expect an RPC from the CLIENT

        Expect an RPC depending on reason for connection (inform RPC)
        After handling RPC, move to SendingRpc state
        For now, expect only done
    """
    def run(self, session):
        return self.handle_message(session, session['rpc']['message'])

    # FIXME parameter session is unused
    def handle_message(self, session, message):
        """ Handle a input from a device """
        # Handle an RPC call
        # Reason should come from inform call.
        response = {}
        if message['method'] == 'done' and message['id'] is None:
            # Here we switch roles, becoming RPC Client
            next_state, response = RPCS.SendingRpc, None
        else:
            # We have a valid method.
            # (VALID_METHODS checked in rpcsd:parse_message)
            next_state = RPCS.ExpectRpc
            response['error'] = {'code': -31998, 'message': 'Wrong request'}
            response['id'] = message['id']

        return next_state, response


class ExpectInform:

    """
        State: Expecting inform RPC call from CLIENT

        Expect an inform RPC call from client containing proper parameters

        Currently: Loads parameters into session and logs connection
        accordingly
    """

    def __init__(self):
        self.earliest_protocol_version = None
        self.latest_protocol_version = None
        self.protocol_version = None
        self.protocol_compression = None

    def run(self, session):
        return self.handle_message(session, session['rpc']['message'])

    def queue_communication(self, session):
        """ Prepares session queue for communication """

        # Here we can queue all communication to be sent to the Client
        # Examples follow...
        session['queue'].append(GetObjects())
        session['queue'].append(DeleteObjects())
        session['queue'].append(RpcExecute())
        session['queue'].append(GetDeviceInfo())

    def parse_protocol_version(self, version_string_list):
        """
            Parses a given protocol string to check if it is in proper
            formatting

            Input parameters:
                version_string_list - list of version strings to be parsed

            Return value:
                version_string      - Malformatted version_string if parsing fails
                                      None in case parsing is successful
        """
        # Verify for every provided string if it is in proper versioning format
        for version_string in version_string_list:

            try:
                parsed_version_string = version_string.split('.')
                if len(parsed_version_string) == 1 and version_string.isdigit():
                    # No dots in version string, it is a simple integer.
                    continue

                StrictVersion(version_string)

            except (AttributeError, ValueError):
                LOG.debug('Invalid protocol version string provided')
                return version_string

            # Check for malformatting
            for i in range(len(parsed_version_string)):
                if len(parsed_version_string[i]) > 1:
                    if parsed_version_string[i][0] == '0':  # Leading 0's
                        return version_string
                if len(parsed_version_string[i]) < 1:  # Empty strings
                    return version_string

        # Protocol version formating: OK
        return None

    def determine_supported_protocol(self, earliest, latest):
        """
            This function determines the common supported protocol version.
            This  is determined by a version supported by the RPCS server
            that is in the range of numbers that exist between the value of
            the first integer of the earliest supported protocol version and
            the value of the first integer of the latest supported protocol
            version.

            Input parameters:
                earliest            - earliest_protocol_version from client Inform
                latest              - latest_protocol_version from client Inform
            Return value:
                protocol_version    - common supported protocol version to be used during session
            Raises:
                NoValidProtocolVersionInCommon - Failed to negotiate common protocol version
        """
        earliest = int(earliest.split('.')[0])
        latest = int(latest.split('.')[0])
        if earliest <= latest:
            supported = range(earliest, latest + 1)
            for version in (reversed(supported)):
                if version in RPCS.SUPPORTED_PROTOCOL_VERSIONS:
                    return str(version)

        # If no common protocol version is found, raise fatal error
        raise ClientRequestError('NoValidProtocolVersionInCommon')

    def compare_protocol_versions(self, session):
        """
            This function is responsible for parsing, validating and making
            all necessary comparisons between provided and supported protocol
            versions.

            Input parameters:
                session - Dictionary with session data (where protocol versions can be found)
            Return value:
                None
            Raises:
                InvalidParameterValue - Error object with invalid protocol version string
                NoValidProtocolVersionInCommon - Failed to negotiate common protocol
        """
        # First parse protocol version strings to check for invalid formatting
        invalid_string = self.parse_protocol_version(
                    [self.earliest_protocol_version, self.latest_protocol_version])
        if invalid_string is not None:
            # Error during protocol string parsing
            data = ('earliest_protocol_version'
                    if invalid_string == self.earliest_protocol_version else 'latest_protocol_version')
            raise ClientRequestError('InvalidParameterValue', data=data)

        # Check if protocol version is supported and define the one to use
        self.protocol_version = self.determine_supported_protocol(
                    self.earliest_protocol_version, self.latest_protocol_version)

    def fetch_inform_params(self, session, params):

        try:
            # First try to fetch mandatory parameters
            self.earliest_protocol_version = get_element('earliest_protocol_version', params,
                mandatory=True,
                data_type=str)

            self.latest_protocol_version = get_element('latest_protocol_version', params,
                mandatory=True,
                data_type=str)

            session['client']['event'] = get_element('event', params, mandatory=True, data_type=str)
            session['client']['cid'] = get_element('clientid', params, data_type=str)
            self.protocol_compression = get_element('protocol_compression', params, data_type=str)
        except (MandatoryFieldNotFoundError, UnexpectedFieldDataType) as err:
            # Get error name as str
            critical = getattr(err, 'critical', None)
            raise ClientRequestError(type(err).__name__, data=err.field, critical=critical)

    def handle_client_id(self, session):
        """ If a client_id is provided together with the inform message,
            Now we have enough information to get the data from the database
        """

        if session['client']['cid'] is not None:
            # A subscriber ID may only contain letters, numbers, spaces and
            # the following special characters: - _ \ / ( ) # .
            p = re.compile('^[A-Za-z0-9-_\\\. #/()]+$')
            if p.match(session['client']['cid']) is None:
                raise ClientRequestError('InvalidClientId')

        try:
            session['client'] = session['db'].client_data_query(
                session['client']['cid'])
        except DbException as db_err:
                session['log'] = {'rc': 'error', 'msg': 'Non matching ClientID'}
                raise ClientRequestError('UnknownClient', data=session['client']['cid'] + ' does not match data in database')

        if session['client'] is None:
            # The client could not be found.
            # It means that the client is not yet defined in the database.
            msg = ' cid:' + session['client']['cid']
            LOG.info("Client not in database, " + msg)
            session['log'] = {'rc': 'ok', 'msg': 'Unknown CLIENT '}
            raise ClientRequestError('UnknownClient', data='No entry for client in database')

    def handle_connection_event(self, session):
        """ Handle connection event """
        # Check if connection event is valid
        if session['client']['event'] not in RPCS.VALID_EVENTS:
            # Critical error: parameter is mandatory
            LOG.debug('Invalid event value in inform message')
            raise ClientRequestError('InvalidParameterValue', data='event', critical=True)
        else:
            self.queue_communication(session)

    def handle_protocol_compression(self, session):
        """
            Validates received protocol compression parameter.
            Sets protocol compression to be used in session according
            to result of this validation.

            Default protocol compression value: 'NONE'
        """
        if self.protocol_compression is not None:
            valid = RPCS.VALID_COMPRESSION_METHODS
            if self.protocol_compression not in valid:
                self.protocol_compression = 'NONE'
                raise ClientRequestError('InvalidParameterValue', data='protocol_compression')
        else:
            self.protocol_compression = 'NONE'

    def handle_inform(self, session, request):

        """
            Handle a request from the device

            Input parameters:
                session  - status and parameters of the current session
                request  - The json formatted message as received from device

            Return value:
                next_state - Either Listening or ExpectRpc, respectively if
                             successful of not
                response   - The json formatted message that must be returned
                             to the device
        """
        # Verify the parameters
        params = get_element('params', request)
        # Default value for protocol_compression
        protocol_compression = 'NONE'
        response = {}

        if params is not None:
            try:
                # Fetch inform parameters and load into session
                self.fetch_inform_params(session, params)
                # handle a possible subscriber id (MACless communication)
                self.compare_protocol_versions(session)
                # If protocol_compression method is provided, check if valid
                self.handle_protocol_compression(session)
                # Validate and check reason (event) for this session
                self.handle_client_id(session)
                # Parse provided protocol version parameters and check validity
                self.handle_connection_event(session)
                # Check for unknown parameters provided in RPC
                for key in params:
                    if key not in RPCS.VALID_INFORM_PARAMETERS:
                        raise ClientRequestError("InvalidParameterName", data=key)

            except ClientRequestError as inform_error:
                next_state = inform_error.error['next_state']
                error_message = {"error": inform_error.error['error']}

                if inform_error.error_name == "InvalidClientId":
                    # As per defined in the protocol: Log in database
                    session['log'] = {'rc': 'error', 'msg': 'Invalid client_id value'}

                LOG.debug("ExpectInform Error: " + str(inform_error))
                return next_state, error_message
            except DbException:
                return (RPCS.ExpectInform, {
                    'error': {'code': -31997,
                              'message': 'Database access error'}})

            # Everything is OK with Inform RPC
            next_state = RPCS.ExpectRpc
            response['result'] = {
                'protocol_version': self.protocol_version,
                'protocol_compression': protocol_compression
            }

        # No parameters provided with inform RPC
        else:
            next_state = RPCS.ExpectInform
            response['error'] = {
                'code': -32602, 'message': 'Invalid parameter'}

        return next_state, response

    def handle_message(self, session, message):

        # Handle an RPC call
        response = None

        if message['method'] == 'inform':
            next_state, response = self.handle_inform(session, message)
        else:
            next_state = RPCS.ExpectInform
            response = {'error': {'code': -31998, 'message': 'Wrong request'}}
        response['id'] = message['id']
        return next_state, response


class Authenticating:

    """ State: Authenticating

        Establish connection to the Database and assign it to session;
        Verify that provided credentials are valid and perform a validation
        check against the Database.
    """

    def connect_database(self, session):
        session['db'] = Db()
        session['db'].connect()

    def prepare_session(self, writer, session):

        """
            Prepare session; attempts database connection, verify provided
            credentials and validate.

            Input parameters:
              session - session to be connected to the database
              writer  - Writing stream to get client information from

            Return values:
              next_state - Either back to Listening or forward to ExpectInform
        """
        # Get information about the CLIENT
        session['client']['ip'], port = writer.get_extra_info('peername')
        # Try Database connection
        try:
            self.connect_database(session)
        except DbException as err:
            LOG.debug('Could not prepare session: {!s}'.format(err))
            return RPCS.Listening

        # Get MAC Address (DNS field) from the certificate
        peercert = writer.get_extra_info('peercert')
        dns = get_element('subjectAltName', peercert)
        try:
            session['client']['did'] = dns[0][1]  # currenlty not used but it should in the certificate
            session['client']['mac'] = dns[1][1]
        except IndexError:
            # Disconnect when credentials cannot be obtained
            LOG.info("Failed to obtain certificate credentials from ip " + session['client']['ip'])
            return RPCS.Listening

        return RPCS.ExpectInform


class Listening:

    def run_forever(self, session_handler, nb_session_handler):
        # The config file contains an IP address to use
        # this must be changed to an interface (client_int)
        # The ip address must then be obtained from the interface
        # [copy from rpcd.pl] 'ifconfig client-int | grep inet |
        #    sed -n 's/.*inet addr:\\([0-9.]\\+\\)\\s.*/\\1/p''
        ip_address = RPCS_INTERFACE
        # using unix domain socket in abstract namespace to avoid
        # permission issues ('\0' before socket path)
        unix_sock_address = '\0/tmp/nb_socket'
        sslcontext = RPCS.context
        loop = asyncio.get_event_loop()

        # start TCP/TLS server for client interface
        asyncio.async(asyncio.start_server(session_handler, ip_address, RPCS_PORT, ssl=sslcontext))
        # start Unix domain socket server for Northbridge interface
        asyncio.async(asyncio.start_unix_server(nb_session_handler, path=unix_sock_address, loop=loop))
        loop.run_forever()


class RPCS:

    # Static RPCS States initialization
    Listening = Listening()
    Authenticating = Authenticating()
    ExpectInform = ExpectInform()
    ExpectRpc = ExpectRpc()
    SendingRpc = SendingRpc()
    ExpectResponse = ExpectResponse()
    # Static RPCS substates that must be executed in a fixed order
    Northbound_Queue = {'udp_id': random.randint(1, 1024)}

    # List of valid values for various attributes
    VALID_METHODS = ['inform', 'set', 'get', 'done']
    VALID_RESULTS = [0, 1]
    VALID_COMPRESSION_METHODS = ['DEFLATE', 'NONE']
    # Add here events which the client can inform the server
    VALID_INFORM_PARAMETERS = ['clientid',
                               'event']

    # Lists of valid Machine States for each Role
    RPC_SERVER_STATES = [ExpectInform, ExpectRpc]
    RPC_CLIENT_STATES = [SendingRpc, ExpectResponse]
    # List of supported protocol versions
    SUPPORTED_PROTOCOL_VERSIONS = [1]

    def __init__(self):
        """
            Create an SSL context suitable for accepting session requests
        """
        try:
            context = ssl.create_default_context(
                purpose=ssl.Purpose.CLIENT_AUTH)
            context.options |= ssl.OP_NO_SSLv2
            context.options |= ssl.OP_NO_SSLv3
            context.options |= ssl.OP_NO_TLSv1
            context.options |= ssl.OP_NO_TLSv1_1
            context.options |= ssl.OP_NO_COMPRESSION
            context.verify_mode = ssl.CERT_REQUIRED
            # TODO do not use static configuration parameters
            context.load_verify_locations(cafile='/sbin/rpcsd/root.cert.pem')
            context.load_cert_chain(certfile='/sbin/rpcsd/gaps.pem')
            context.set_ciphers('AES128-SHA256')
            RPCS.context = context
        except FileNotFoundError:
            # If we can't set up TLS context, log error and exit
            LOG.error("Could not setup TLS context: certificate file(s) "
                      "not present in the correct directory")
            exit(1)
