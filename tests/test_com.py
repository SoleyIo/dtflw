import unittest
from unittest.mock import patch
import dtflw.com as com
import tests.utils as utils
import dtflw.arguments as a
from ddt import ddt, data, unpack


@ddt
class MessagingTestCase(unittest.TestCase):

    @patch("dtflw.databricks.get_spark_session")
    def test_share_arguments(self, get_session_mock):

        # Arrange
        get_session_mock.return_value = utils.SparkSessionMock()
        ch = com.NotebooksChannel()

        arguments = {
            "foo": {
                "name": "foo",
                "value": "42",
                "name_suffix": a.Argument.NAME_SUFFIX
            },
            "A_in": {
                "name": "A",
                "value": "data/A.parquet",
                "name_suffix": a.Input.NAME_SUFFIX
            },
            "B_out": {
                "name": "B",
                "value": "data/B.parquet",
                "name_sufffx": a.Output.NAME_SUFFIX
            }
        }
        nb_abs_path = "/Repos/user@a.b/project/nb"

        # Act
        ch.share_arguments(nb_abs_path, arguments)

        # Assert
        actual = ch.try_get_arguments(nb_abs_path)
        self.assertDictEqual(arguments, actual)

    @patch("dtflw.databricks.get_spark_session")
    def test_try_get_arguments_default(self, get_session_mock):

        get_session_mock.return_value = utils.SparkSessionMock()
        ch = com.NotebooksChannel()

        actual = ch.try_get_arguments("nb")

        self.assertDictEqual({}, actual)


@ddt
class MessageBusTestCase(unittest.TestCase):

    @patch("dtflw.databricks.get_spark_session")
    def test_share_topic_exists(self, get_session_mock):

        # Arrange
        get_session_mock.return_value = utils.SparkSessionMock()
        bus = com.MessageBus()
        bus.share("channel_1", "topic_1", "foo")

        # Act
        bus.share("channel_1", "topic_1", "bar")

        # Assert
        actual = bus.try_get()
        self.assertDictEqual(
            {
                "channel_1": {
                    "topic_1": "bar"
                }
            },
            actual
        )

    @patch("dtflw.databricks.get_spark_session")
    def test_share_channel_exists(self, get_session_mock):

        # Arrange
        get_session_mock.return_value = utils.SparkSessionMock()
        bus = com.MessageBus()
        bus.share("channel_1", "topic_1", "foo")

        # Act
        bus.share("channel_1", "topic_2", "bar")

        # Assert
        actual = bus.try_get()
        self.assertDictEqual(
            {
                "channel_1": {
                    "topic_1": "foo",
                    "topic_2": "bar"
                }
            },
            actual
        )

    @data(
        (None, None, None, {
            "channel_1": {
                "topic_11": "foo",
                "topic_12": "bar"
            },
            'channel_2': {
                "topic_21": "baz"
            }
        }),
        ("channel_1", None, None, {
            "topic_11": "foo",
            "topic_12": "bar"
        }),
        ("channel_2", "topic_21", None, "baz"),
        # Returns default
        ("channel_1", "topic_unknown", 42, 42),
        ("channel_unknown", "topic_1", 42, 42),
    )
    @unpack
    @patch("dtflw.databricks.get_spark_session")
    def test_try_get(self, channel, topic, default, expected, get_session_mock):

        # Arrange
        get_session_mock.return_value = utils.SparkSessionMock()
        bus = com.MessageBus()
        bus.share("channel_1", "topic_11", "foo")
        bus.share("channel_1", "topic_12", "bar")
        bus.share("channel_2", "topic_21", "baz")

        # Act
        actual = bus.try_get(channel, topic, default)

        # Assert
        self.assertEqual(expected, actual)
