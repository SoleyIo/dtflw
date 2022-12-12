from dtflw.flow import Flow, FlowContext, LazyNotebook
import unittest
from dtflw.logger import DefaultLogger
from dtflw.plugin import FlowPluginBase, NotebookPluginBase


class PluginTestCase(unittest.TestCase):

    class TestFlowPlugin(FlowPluginBase):

        @property
        def action_name(self):
            return "do_new_stuff"

        def act(self, flow: Flow, name):
            """
            Greets.
            """
            return f"Hello: {name}"

    class TestNotebookPlugin(NotebookPluginBase):

        @property
        def action_name(self):
            return "get_arg_value"

        def act(self, notebook: LazyNotebook, flow: Flow, arg_name: str):
            """
            Returns arg name.
            """
            return notebook._LazyNotebook__args[arg_name]

    def test_install_flow_plugin_ok(self):

        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = self.TestFlowPlugin()
        expected = plg.act(flow, "Alice")

        # Act
        flow.install(plg)
        actual = flow.do_new_stuff("Alice")

        # Assert
        self.assertEqual(expected, actual)

    def test_install_wrong_plugin_type(self):

        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        # Act/Assert
        with self.assertRaises(ValueError):
            flow.install("this is wrong plugin type")

    def test_install_notebook_plugin_ok(self):
        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = self.TestNotebookPlugin()
        expected = "hey there!"

        # Act
        flow.install(plg)

        actual = (
            flow.notebook("import_data")
                .args({"a1": expected})
                .get_arg_value("a1")
        )

        # Assert
        self.assertEqual(expected, actual)

    def test_install_notebook_plugin_twice_fails(self):
        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = self.TestNotebookPlugin()

        flow.install(plg)

        # Act/Assert
        with self.assertRaises(ValueError):
            flow.install(plg)
