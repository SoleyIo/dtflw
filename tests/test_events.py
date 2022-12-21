import unittest
from dtflw.events import EventDispatcher, EventHandlerBase


class TestEventHandler(EventHandlerBase):
    def __init__(self, state=None):
        self.state = state

    def handle(self, new_state):
        self.state = new_state


class EventDispatcherTestCase(unittest.TestCase):

    def test_fire(self):

        # Arrange
        d = EventDispatcher()

        h1 = TestEventHandler(1)
        h2 = TestEventHandler(2)

        d.subscribe("testing", h1)
        d.subscribe("testing", h2)

        # Act: fire all
        d.fire("testing", 3)

        # Assert
        self.assertEqual(h1.state, 3)
        self.assertEqual(h2.state, 3)

        # Act: unsubscribe and fire
        d.unsubscribe("testing", h1)
        d.fire("testing", 4)

        # Assert
        self.assertEqual(h1.state, 3)
        self.assertEqual(h2.state, 4)

        # Act: unsubscribe and fire
        d.unsubscribe("testing", h2)
        d.fire("testing", 5)

        # Assert
        self.assertEqual(h1.state, 3)
        self.assertEqual(h2.state, 4)

    def test_fire_unknown(self):
        d = EventDispatcher()
        d.fire("unknown")

    def test_unsubscribe_unknown(self):
        d = EventDispatcher()
        h = TestEventHandler()
        d.unsubscribe("unknown", h)

    def test_unsubscribe_all(self):
        # Arrange
        d = EventDispatcher()
        h = TestEventHandler()

        d.subscribe("event", h)
        d.fire("event", "foo")

        # Act
        d.unsubscribe_all()
        d.fire("event", "bar")

        # Assert
        self.assertEqual(h.state, "foo")
