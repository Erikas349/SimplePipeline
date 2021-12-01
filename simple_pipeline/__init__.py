"""
This file is part of SimplePipeline.

SimplePipeline is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SimplePipeline is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
Lesser GNU General Public License for more details.

You should have received a copy of the Lesser GNU General Public License
along with SimplePipeline. If not, see <http://www.gnu.org/licenses/>.

Copyleft 2021 - present, Lucas Liendo.
"""

import queue
import sys

from multiprocessing import Event, JoinableQueue, Process, Queue


class Processor(Process):
    """
    A generic processor that performs arbitrary work.

    Clients of this class should inherit and implement the `process` method.
    Optionally the `on_start` and `on_exit` might be implemented.

    :param queues: A dictionary containing three queues under the `input`,
        `output` and `error` keys.
    :param: An `Event` object. It is used to assert that this `Process`
        instance has finished.
    :param: A `Stage` object. Used to check if the previous stage has
        finished.
    :param args: Arbitrary user supplied arguments.
    :param kwargs: Arbitrary user supplied keyword arguments.
    """

    def __init__(self, queues, stop_event, previous_stage, *args, **kwargs):
        self.queues = queues
        self.stop_event = stop_event
        self.previous_stage = previous_stage
        self.args = args or None
        self.kwargs = kwargs or None
        super().__init__()

    def _is_producer(self):
        return (self.queues['input'] is None) and (self.queues['output'] is not None)

    def _is_consumer(self):
        return (self.queues['output'] is None) and (self.queues['input'] is not None)

    def process(self, item=None):
        """
        Implemented by subclasses.

        If the class which inherits is:
            - A producer: it must return values through a generator using the
              the `yield` keyword.
            - A consumer/producer: Must process the incoming item and return
              it for the next stage.
            - A consumer: Should consume the item and perform an action that
              leaves the result outside the pipeline as no other stages are
              available.

        :param item: An arbitrary object to be processed.
        """
        raise NotImplementedError()

    def handle_error(self, error):
        self.queues['error'].put_nowait(error)

    def _items_to_process(self):
        return not self.previous_stage.finished

    def _consume(self):
        while self._items_to_process():
            try:
                input_item = self.queues['input'].get_nowait()
                self.queues['input'].task_done()
                self.process(item=input_item)
            except queue.Empty:
                pass
            except Exception as error:
                self.handle_error(error)

        self.stop_event.set()

    def _consume_and_produce(self):
        while self._items_to_process():
            try:
                input_item = self.queues['input'].get_nowait()
                self.queues['input'].task_done()
                self.queues['output'].put_nowait(
                    self.process(item=input_item)
                )
            except queue.Empty:
                pass
            except Exception as error:
                self.handle_error(error)

        self.queues['output'].join()
        self.stop_event.set()

    def _produce(self):
        try:
            for output_item in self.process():
                self.queues['output'].put_nowait(output_item)
        except Exception as error:
            self.handle_error(error)

        self.queues['output'].join()
        self.stop_event.set()

    def on_start(self):
        """
        Optional method that gets called once before processing items.
        """

    def run(self):
        self.on_start()

        if self._is_producer():
            self._produce()
        elif self._is_consumer():
            self._consume()
        else:
            self._consume_and_produce()

        self.on_exit()

    def on_exit(self):
        """
        Optional method that gets called once after this process finishes.
        """


class Stage:
    """
    Group and control a set of processes.

    :param config: A dictionary containing the configuration for a `Processor`.
    :param queues: A dictionary containing three queues under the
        "input", "output" and "error" keys.
    :param previous_stage: A `Stage` object representing the previous stage
        of the pipeline (or None if it is the first one).
    """

    def __init__(self, config, queues, previous_stage):
        processor = config['processor']
        args = config.get('args', ())
        kwargs = config.get('kwargs', {})
        total = config.get('total', 1)

        self.stop_events = [Event() for _ in range(total)]
        self.processors = [
            processor(
                queues, stop_event, previous_stage,
                *args, **kwargs,
            )
            for stop_event in self.stop_events
        ]

    @property
    def finished(self):
        return all([stop_event.is_set() for stop_event in self.stop_events])

    def start(self):
        for processor in reversed(self.processors):
            processor.start()

    def join(self):
        for processor in reversed(self.processors):
            processor.join()


class ErrorWatcher(Process):
    """
    Monitor and process the error queue.

    :param stages: A list of `Stage` objects.
    :param error_queue: A `Queue` object. This is the error queue where all processes
        report errors.
    """

    def __init__(self, stages, error_queue):
        self.stages = stages
        self.error_queue = error_queue
        super().__init__()

    def _items_to_process(self):
        return not all([stage.finished for stage in self.stages])

    def process(self, error):
        """
        Handle the received error.

        By default this method will simply output the error to `stderr` but it
        can be fully overridden to handle errors differently (e.g: log to a file).

        :param error: An `Exception` object.
        """
        print(error, file=sys.stderr)

    def on_start(self):
        """
        Optional method that gets called once before this process starts.
        """

    def run(self):
        self.on_start()

        while self._items_to_process():
            try:
                error = self.error_queue.get_nowait()
                self.process(error)
            except queue.Empty:
                pass

        self.on_exit()

    def on_exit(self):
        """
        Optional method that gets called once after this process finishes.
        """


class PipelineError(Exception):
    pass


class Pipeline:
    """
    A multiprocess pipeline.

    :param config: A list of dictionaries. Each dictionary must at least
        contain the "processor" key and its value must be a subclass of
        `Processor`.

        Other optional keys are:
            * total: An integer indicating the total number of proceses to be spawn
              for that stage.
            * args: A tuple containing arbitrary arguments to be passed down to
              a `Processor`.
            * kwargs: A dictionary containing arbitrary keyword arguments to be
              passed down to a `Processor`.
    :param error_watcher: An optional subclass of `ErrorWatcher` to handle errors
        in a custom way.
    """

    minimum_stages = 2

    def __init__(self, config, error_watcher=None):
        total_stages = len(config)

        if total_stages < self.minimum_stages:
            raise PipelineError(
                f"At least {self.minimum_stages} stages are required by `Pipeline`."
            )

        self.error_queue = Queue()
        self.queues = self._build_queues(total_stages)
        self.stages = self._build_stages(config)
        self.error_watcher = self._build_error_watcher(error_watcher or ErrorWatcher)

    def _build_error_watcher(self, error_watcher):
        return error_watcher(self.stages, self.error_queue)

    def _build_queues(self, total_stages):
        return [None, *[JoinableQueue() for _ in range(total_stages - 1)], None]

    def _build_stage(self, stage_number, stage_config, previous_stage):
        return Stage(
            stage_config,
            self._get_queues_for_stage(stage_number),
            previous_stage,
        )

    def _build_stages(self, config):
        stages = [self._build_stage(0, config[0], None)]  # Build the first stage.

        for stage_number, stage_config in enumerate(config[1:], start=1):
            stages.append(
                self._build_stage(
                    stage_number,
                    stage_config,
                    stages[stage_number - 1],
                )
            )

        return stages

    def _get_queues_for_stage(self, stage_number):
        return {
            'input': self.queues[stage_number],
            'output': self.queues[stage_number + 1],
            'error': self.error_queue,
        }

    def start_stages(self):
        self.error_watcher.start()

        for stage in reversed(self.stages):
            stage.start()

    def join_stages(self):
        self.error_watcher.join()

        for stage in reversed(self.stages):
            stage.join()

    def run(self):
        self.start_stages()
        self.join_stages()
