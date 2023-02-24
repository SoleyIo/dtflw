import dtflw.databricks as db
import json


class MessageBus:
    """
    Implements a simple messaging via `spark.cong` object.
    A bus object is a JSON of the structure:

    ROOT_CHANNEL: {
        channel: {
            topic: message
        }
    }
    """
    ROOT_CHANNEL = "flow.bus"

    def share(self, channel: str, topic: str, message):
        """
        Shares a message of a given topic on a given channel. If there is already a message
        under channel-topic then it will be replaced with a given one.

        Parameters
        ----------
        channel: str
            Name of a channel.
        topic: str
            Name of a topic.
        message: object
            A message.
        """
        if channel is None or len(channel) == 0:
            raise ValueError("`channel` cannot be None nor an empty string.")

        if topic is None or len(topic) == 0:
            raise ValueError("`topic` cannot be None nor an empty string.")

        bus = {}
        if db.runtime_config_has(self.ROOT_CHANNEL):
            bus = json.loads(db.get_runtime_config_property(self.ROOT_CHANNEL))

        if channel not in bus:
            bus[channel] = {}

        bus[channel][topic] = message

        db.set_runtime_config_property(self.ROOT_CHANNEL, json.dumps(bus))

    def try_get(self, channel: str = None, topic: str = None, defualt=None):
        """
        Returns a message under a given channel-topic if it exists.
        Otherwise, returns `default`.

        If `channel` is None then returns the complete bus content.
        If `topic` is None then returns the complete `channel` content.
        """

        if not db.runtime_config_has(self.ROOT_CHANNEL):
            return defualt

        bus = json.loads(db.get_runtime_config_property(self.ROOT_CHANNEL))

        if channel is None:
            return bus

        if channel not in bus:
            return defualt

        if topic is None:
            return bus[channel]

        if topic not in bus[channel]:
            return defualt

        return bus[channel][topic]


class NotebooksChannel:
    """
    Implements a channel specific to messaging of notebooks related content.
    """

    CHANNEL = "notebooks"

    def __init__(self):
        self.__bus = MessageBus()

    def __share(self, notebook_abs_path, sub_topic: str, message):

        existing_message = self.__get(notebook_abs_path)
        updated_message = {**existing_message, **{sub_topic: message}}

        self.__bus.share(self.CHANNEL, notebook_abs_path, updated_message)

    def share_args(self, notebook_abs_path: str, args):
        self.__share(notebook_abs_path, "args", args)

    def share_inputs(self, notebook_abs_path: str, inputs):
        self.__share(notebook_abs_path, "inputs", inputs)

    def share_outputs(self, notebook_abs_path: str, outputs):
        self.__share(notebook_abs_path, "outputs", outputs)

    def __get(self, notebook_abs_path: str, sub_topic: str = None):
        message = self.__bus.try_get(self.CHANNEL, notebook_abs_path, {})
        if sub_topic is None:
            return message
        return message[sub_topic] if sub_topic in message else {}

    def get_args(self, notebook_abs_path: str):
        return self.__get(notebook_abs_path, "args")

    def get_inputs(self, notebook_abs_path: str):
        return self.__get(notebook_abs_path, "inputs")

    def get_outputs(self, notebook_abs_path: str):
        return self.__get(notebook_abs_path, "outputs")
