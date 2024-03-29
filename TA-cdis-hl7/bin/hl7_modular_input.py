import os
import sys
import threading
from Queue import Queue

APP_NAME="TA-cdis-hl7"

path_to_python_packages= os.path.join(os.path.dirname(os.path.abspath(__file__)), APP_NAME)
sys.path.insert(0, path_to_python_packages)

path_to_mod_input_lib = os.path.join(os.path.dirname(os.path.abspath(__file__)), APP_NAME,  'modular_input.zip')
sys.path.insert(0, path_to_mod_input_lib)

from modular_input import Field, BooleanField, ListField, IntegerField

from hl7apy.core import Message
from hl7apy.mllp import MLLPServer
#from mllp2 import MLLPServer

from hl7apy.mllp import AbstractErrorHandler
from hl7apy.parser import parse_message

import time
import calendar




# (monkey) patching ModularInput
import modular_input

class ModularInput(modular_input.ModularInput):

    def output_event(self, data_dict, stanza, _time=None, index=None, sourcetype=None, source=None, host=None, unbroken=False,
                     close=False, out=sys.stdout, encapsulate_value_in_double_quotes=False):
        output = self.create_event_string(data_dict, stanza, _time, sourcetype, source, index, host,
                                          unbroken, close,
                                          encapsulate_value_in_double_quotes=encapsulate_value_in_double_quotes)
        self.logger.debug("the xml file looks like \n %s", output)

        with self.lock:
            out.write(output)
            out.flush()

    def create_event_string(self, data_dict, stanza, _time, sourcetype, source, index, host=None,
                            unbroken=False, close=False, encapsulate_value_in_double_quotes=False):
        """
        Create a string representing the event.

        Argument:
        data_dict -- A dictionary containing the fields
        stanza -- The stanza used for the input
        sourcetype -- The sourcetype
        source -- The source field value
        index -- The index to send the event to
        unbroken --
        close --
        encapsulate_value_in_double_quotes -- If true, the value will have double-quotes added around it.
        """

        """
        My change to the original function:
        1. allow raw data and not necessarily key-value pairs. 
        2. 
        """

        # Make the content of the event
        data_str = ''

        if isinstance(data_dict,(str, unicode)):
            data_str=data_dict
        else:
            for k, v in data_dict.items():

                # If the value is a list, then write out each matching value with the same name (as mv)
                if isinstance(v, list) and not isinstance(v, basestring):
                    values = v
                else:
                    values = [v]

                k_escaped = self.escape_spaces(k)

                # Write out each value
                for v in values:
                    v_escaped = self.escape_spaces(v, encapsulate_in_double_quotes=encapsulate_value_in_double_quotes)

                    if len(data_str) > 0:
                        data_str += ' '

                    data_str += '%s=%s' % (k_escaped, v_escaped)

        # Make the event
        event_dict = {'stanza': stanza,
                      'data' : data_str}

        if index is not None:
            event_dict['index'] = index

        if sourcetype is not None:
            event_dict['sourcetype'] = sourcetype

        if source is not None:
            event_dict['source'] = source

        if host is not None:
            event_dict['host'] = host

        if _time is not None:
            event_dict['time'] = _time

        #self.streaming_mode ="true"
        event = self._create_event(self.document,
                                   params=event_dict,
                                   stanza=stanza,
                                   unbroken=unbroken,
                                   close=close)

        # If using unbroken events, the last event must have been
        # added with a "</done>" tag.
        return self._print_event(self.document, event)


class SocketPortField(Field):
    """
    A validator that converts a socket port to a integer.
    """

    def to_python(self, value, session_key=None):

        Field.to_python(self, value, session_key)

        if value is not None:
            try:
                port=int(value)
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex(('127.0.0.1',port))
                if result == 0:
                    #print "Port is open"
                    raise Exception(value + " is already in use")
                else:
                    #print "Port is not open"
                    return port
                return float(value)
            except ValueError as exception:
                raise FieldValidationException(str(exception))
        else:
            return None

    def to_string(self, value):

        if value is not None:
            return str(value)

        return ""

    def get_data_type(self):
        return Field.DATA_TYPE_NUMBER




class MyInput(ModularInput):



    def __init__(self, timeout=30):
        scheme_args = {'title': "hl7_modular_input",
                       'description': "Get data from HL7 senders",
                       'use_external_validation': "true",
                       'streaming_mode': "xml",
                       'use_single_instance': "false"}  # let splunk create one instance per stanza.

        args = [
            Field("title", "Title", "A short description of the input", empty_allowed=False),
            SocketPortField("port", "Port", "The Port to listen at", empty_allowed=False),
            BooleanField("output_kvp", "Output key value pair", "Output key value pair instead", empty_allowed=True),
            BooleanField("remove_phi", "Remove PHI", "Whether to remove common PHI segments", empty_allowed=True),
            ListField("fields_to_remove", "Fields to remove", "Extra segments or fields to remove", empty_allowed=True, required_on_create=False),
            IntegerField("max_long_segment","Max bytes for segments", "segment longer than this number of bytes will be removed", empty_allowed=True, none_allowed=True)
        ]

        # the mllp server
        # 1. one per stanza, created upon first run, see below.
        # 2. interval parameter should not be configured when creating modular inputs, otherwise
        #    server will be recreated every interval?
        self.mllp = None

        self.sleep_interval = 5

        self._queue = Queue()

        ModularInput.__init__(self, scheme_args, args, logger_name='hl7_modular_input')

    def start_mllp_server(self):

        try:
            self.mllp.serve_forever()
        except KeyboardInterrupt as ki:
            self.mllp.shutdown()
            self.mllp=None
        except Exception as e:
            self.mllp=None
            self.logger.info("Some error occured: %s", e)

    def en_queue(self, data):
        self._queue.put(data)


    def run(self, stanza, cleaned_params, input_config):
        #interval = cleaned_params["interval"]
        interval = cleaned_params.get("interval",5)
        title = cleaned_params["title"]
        host = cleaned_params.get("host", None)
        index = cleaned_params.get("index", "default")
        sourcetype = cleaned_params.get("sourcetype", "hl7")

        port = cleaned_params["port"]
        output_kvp = cleaned_params.get("output_kvp", False)
        remove_phi = cleaned_params.get("remove_phi", False)
        fields_to_remove = cleaned_params.get("fields_to_remove", [])
        max_long_segment = cleaned_params.get("max_long_segment", 0)


        # because we are forcing multiple instances,  we need to keep it running
        # otherwise the main thread will exit and the spawned mllp server will die with it
        # The first option is doing a forever loop here.
        # The second option is making the mllp server run in another daemon thread

        if self.mllp is None:
            handlers = {
                'ERR': (CatchAllHandler,self)
            }
            # the mllp server is not running, try starting it

            self.mllp = MLLPServer('0.0.0.0', port, handlers )

            self.mllp_thread = threading.Thread(target=self.start_mllp_server)
            self.mllp_thread.daemon = True
            self.logger.info("Starting MLLP Server for stanza=%s", stanza)
            self.mllp_thread.start()

        while True:
            while not self._queue.empty():
                t0, t1=self._queue.get()
                #self.logger.info("about to process this message before storing it %s", t0)

                # raw_test= ("MSH|^~\&|EPIC|EPICADT|SMS|SMSADT|199912271408|CHARRIS|ADT^A04|1817457|D|2.5|\r" +
                #     "PID||0493575^^^2^ID 1|454721||DOE^JOHN^^^^|DOE^JOHN^^^^|19480203|M||B|254 MYSTREET AVE^^MYTOWN^OH^44123^USA||(216)123-4567|||M|NON|400003403~1129086|999-|\r"
                #     +"NK1||ROE^MARIE^^^^|SPO||(216)123-4567||EC|||||||||||||||||||||||||||\r"
                #     +"NK1||DOE^JOHN ^^^^|SPO||(216)123-4567||EC|||||||||||||||||||||||||||\r"
                #     +"NK1||DOE^ROBERT ^^^^|SPO||(216)123-4568||EC|||||||||||||||||||||||||||\r"
                #     +"PV1||O|168 ~219~C~PMA^^^^^^^^^||||277^ALLEN MYLASTNAME^BONNIE^^^^|||||||||| ||2688684|||||||||||||||||||||||||199912271408||||||002376853")
                # t1 = parse_message(raw_test)
                # t1.name="unamed"

                #todo: option to splunk parsed data as kvp
                #todo: option to chooce timestamp
                #todo: option to de-identify the data based on default or user-supplied routines
                #todo: option to filter out large segments



                t2= ["    "*level+ (n.name or "unamed")  +"="+ hl7_util.escape_spaces(n.to_er7())+ "," for [n, level] in hl7_util.preorder(t1)]

                t3="\n".join(t2)

                out = t0
                self.logger.info("about to send this to splunk\n %s", out)
                self.output_event(out, stanza, _time=calendar.timegm(time.gmtime()), sourcetype=sourcetype, host=host, index=index)

            time.sleep(0.1)

        # if self.needs_another_run(input_config.checkpoint_dir, stanza, interval):
        #     pass
        #     #self.logger.debug("Your input should do something here, stanza=%s", stanza)


class hl7_util:

    @staticmethod
    def escape_spaces( s, encapsulate_in_double_quotes=True):
        """
        If the string contains spaces or is empty, then add double quotes around the string. This
        is useful when outputting fields and values to Splunk since a space will cause Splunk to
        not recognize the entire value.

        Arguments:
        s -- A string to escape.
        encapsulate_in_double_quotes -- If true, the value will have double-spaces added around it.
        """

        # Make sure the input is a string
        if s is not None:
            s = str(s)

        # Escape the spaces within the string (will need KV_MODE = auto_escaped for this to work)
        if s is not None:
            s = s.replace('"', '\\"')
            s = s.replace("'", "\\'")

        if s is not None and (" " in s or encapsulate_in_double_quotes or s == ""):
            return '"' + s + '"'

        else:
            return s

    @staticmethod
    def clone(original):
        """
                The library used doesn't provide cloning directly. So this is a hack.
        :param original: The original message in parsed format
        :return: deeply cloned message

        """
        cloned = parse_message(original.to_er7())

        return cloned

    # @staticmethod
    # def preorder(root):
    #     if root is not None:
    #         yield root
    #         for child in root.children:
    #             for key in hl7_util.preorder(child):
    #                 yield key

    @staticmethod
    def preorder(root, level=0):
        """

        :param root: traverse from
        :param level: current level
        :return: generator that enumerates all components of a message
        """
        if root is not None:
            yield [root, level]
            for child in root.children:
                for [key, nextlevel] in hl7_util.preorder(child, level=level+1):
                    yield [key, nextlevel]

    @staticmethod
    def setvalue(m, field_or_segment, value):
        from string import split

        try :
            components= split(field_or_segment,".")

            if len(components)==1 :
                setattr(m,components[0],value)
            else:
                setattr(getattr(m, components[0]), components[1], value)
        except:

            pass


    @staticmethod
    def delvalue(m, field_or_segment):
        from string import split

        try:
            components= split(field_or_segment,".")

            if len(components) == 1:
                delattr(m, components[0])
            else:
                delattr(getattr(m, components[0]), components[1])

        except:
            pass


class CatchAllHandler(AbstractErrorHandler):

    def ack(self, message):
        """
        Build a ack response for the incoming message

        :param message: incoming message
        :return: a NAK message
        """
        response = Message("ACK")
        response.MSH.MSH_9 = "ACK"  # message type
        response.MSA.MSA_1 = "AA"   # accept
        response.MSA.MSA_2 = message.MSH.MSH_10  # the original message's control ID
        response.MSA.MSA_3 = "received by splunk"  # any text message
        return response

    def __init__(self, ex, msg, mi):
        super(CatchAllHandler, self).__init__(ex, msg)
        self.mi = mi

    def reply(self):

        msg = parse_message(self.incoming_message)

        # put the message into the queue and don't wait for splunking.
        self.mi.en_queue((self.incoming_message,msg))
        # do something with the message

        res = self.ack(msg)
        res_mllp=res.to_mllp()

        self.mi.logger.debug("about to send this replay to the client: \n %s", res_mllp)

        return res_mllp




if __name__ == '__main__':
    MyInput.instantiate_and_execute()
