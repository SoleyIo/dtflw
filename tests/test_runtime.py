import unittest
from dtflw.runtime import NotebookRun, Runtime

class RuntimeTestCase(unittest.TestCase):

    def test_add_run(self):
        # Arrange
        runtime = Runtime()

        # Act
        r1 = NotebookRun(
            "notebook_1",
            {"arg": "val"},
            {"input": "input_path"},
            {"output": "output_path"}
        )
        runtime.add_run(r1)

        # Assert
        actual_run = list(runtime.runs)[0]

        self.assertEqual("notebook_1", actual_run.notebook_path)
        self.assertEqual({"arg": "val"}, actual_run.args)
        self.assertEqual({"input": "input_path"}, actual_run.inputs)
        self.assertEqual({"output": "output_path"}, actual_run.outputs)
