import unittest
from unittest.mock import patch
from dtflw.flow import Flow
from dtflw.flow_context import FlowContext
from dtflw.lazy_notebook import LazyNotebook
from dtflw.logger import DefaultLogger
from dtflw.plugin import FlowPluginBase, NotebookPluginBase


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
        return notebook.get_args()[arg_name]

class TestNotebookPlugin2(NotebookPluginBase):

    @property
    def action_name(self):
        return "get_args_count"

    def act(self, notebook: LazyNotebook, flow: Flow, arg_name: str):
        """
        Returns arg name.
        """
        return len(notebook.get_args())


class FlowTestCase(unittest.TestCase):

    def test_install_flow_plugin(self):

        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = TestFlowPlugin()
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
            flow.install("This is wrong plugin type")

    def test_install_notebook_plugin(self):
        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = TestNotebookPlugin()
        expected = "foo"

        # Act
        flow.install(plg)

        actual = (
            flow.notebook("import_data")
                .args({"a": expected})
                .get_arg_value("a")
        )

        # Assert
        self.assertEqual(expected, actual)

    def test_install_notebook_plugin_twice_fails(self):
        # Arrange
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        plg = TestNotebookPlugin()

        flow.install(plg)

        # Act/Assert
        with self.assertRaisesRegex(ValueError, f"Notebook plugin {plg.action_name} is already installed."):
            flow.install(TestNotebookPlugin())

    def test_notebook(self):

        # Arrange
        nb_plg = TestNotebookPlugin()
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)
        flow.install(nb_plg)

        # Act
        nb = flow.notebook("project/nb")

        # Assert
        self.assertEqual(nb.rel_path, "project/nb")
        self.assertTrue(hasattr(nb, nb_plg.action_name))

    def test_insall_two_notebook_plugins(self):
        # Arrange
        nb_plg1 = TestNotebookPlugin()
        nb_plg2 = TestNotebookPlugin2()
        ctx = FlowContext(None, None, None, DefaultLogger())
        flow = Flow(ctx)

        flow.install(nb_plg1)
        flow.install(nb_plg2)

        # Act
        expected1 = "foo"
        expected2 = 1

        actual1 = (
            flow.notebook("import_data")
                .args({"a": expected1})
                .get_arg_value("a")
        )

        actual2 = (
            flow.notebook("import_data")
                .args({"a": expected2})
                .get_args_count("a")
        )

        # Assert
        self.assertEqual(expected1, actual1)
        self.assertEqual(expected2, actual2)
