import unittest
from dtflw.pipeline import NotebookRun, PipelineState

class PipelineStateTestCase(unittest.TestCase):

    def test_record_run(self):
        # Arrange
        p = PipelineState()

        # Act
        r1 = NotebookRun(
            "notebook_1",
            {"arg": "val"},
            {"input": "input_path"},
            {"output": "output_path"}
        )
        p.record_run(r1)

        # Assert
        actual_run = list(p.runs)[0]

        self.assertEqual("notebook_1", actual_run.notebook_path)
        self.assertEqual({"arg": "val"}, actual_run.args)
        self.assertEqual({"input": "input_path"}, actual_run.inputs)
        self.assertEqual({"output": "output_path"}, actual_run.outputs)
