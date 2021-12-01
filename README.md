Simple Pipeline
===============

Simple pipeline is a tiny Python module that allows you to create and run multi-process
pipelines. This is useful if you need to process large amount of items and want to exploit
the multiprocessing power of your computer.


Installation
------------

Clone this repository to a temporary directory using [GIT](https://git-scm.com/) (or alternatively
download as [.zip](https://github.com/lliendo/SimplePipeline/archive/master.zip) and run:

```bash
git clone https://github.com/lliendo/SimplePipeline.git
cd SimplePipeline
python setup.py install
```


Documentation
-------------

Simple pipeline basically consists of three Python classes: `Process`, `Stage` and `Pipeline`.

To create a pipeline first design your "processor" classes. These classes might fall under one
of these categories: producer, consumer/producer or consumer. Simple pipeline knows under which
category a processor class falls automatically.

Once you designed all your processor classes just configure and run an instance of `Pipeline`.
For example:

```python
Pipeline([
    {'processor': Producer},
    {'processor': Multiplier, 'total': 4},  # consumer/producer.
    {'processor': Consumer, 'total': 4},
]).run()
```

The above snippet creates a pipeline that consists of a single producer (the `Producer` class),
four consumer/producers (the `Multiplier` class) and finally four consumers (the `Consumer` class).

Each these of the above classes (`Producer`, `Multiplier` and `Consumer`) inherit from
the `Processor` class.

The first argument for `Pipeline` expects a list of dictionaries. Each dictionary must
at least include the `processor` keyword and a `Processor` class as value. If the `total` keyword
argument is not supplied then the total number of processes for that stage will be one.

Optionally the `args` and `kwargs` keys can be supplied to pass down arbitrary arguments and keyword
arguments to each processor. For example:

```python
Pipeline([
    {
        'processor': Producer,
        'args': (1, 2, ...),
        'kwargs': {'kwarg': 'A', ...}
    },
    ...
]).run()
```

Then any `Producer` method can access these attributes through `self.args` and `self.kwargs`.
The final `run()` call on the `Pipeline` instance triggers the pipeline.

Here is a complete example:

```python
"""
The following dummy example shows how a pipeline should be setup.
Some random failures are intentionally added to the `Multiplier` and `Consumer`
classes to demonstrate how exceptions are caught and sent to the error queue.
"""

import os
import random

from simple_pipeline import Processor, Pipeline


class Producer(Processor):

    TOTAL_ITEMS = 1000

    def on_start(self):
        print(f'Producer PID: {os.getpid()} starting.')
        self.total_processed = 0

    def process(self, item=None):
        for n in range(self.TOTAL_ITEMS):
            self.total_processed += 1
            yield n

    def on_exit(self):
        print(f'Producer PID: {os.getpid()} finishing. Total produced: {self.total_processed}.')


class MultiplierError(Exception):
    pass


class Multiplier(Processor):
    def on_start(self):
        print(f'Multiplier PID: {os.getpid()} starting.')
        self.total_processed = 0

    def process(self, item=None):
        if random.random() <= 0.01:
            raise MultiplierError('Multiplier exception raised.')

        self.total_processed += 1
        output_value = item * 2

        return output_value

    def on_exit(self):
        print(f'Multiplier PID: {os.getpid()} finishing. Total multiplied: {self.total_processed}.')


class ConsumerError(Exception):
    pass


class Consumer(Processor):
    def on_start(self):
        print(f'Consumer PID: {os.getpid()} starting.')
        self.total_processed = 0

    def process(self, item=None):
        if random.random() <= 0.01:
            raise ConsumerError('Consumer exception raised.')

        self.total_processed += 1

    def on_exit(self):
        print(f'Consumer PID: {os.getpid()} finishing. Total consumed: {self.total_processed}.')


if __name__ == '__main__':
    Pipeline([
        {'processor': Producer},
        {'processor': Multiplier, 'total': 4},
        {'processor': Consumer, 'total': 4},
    ]).run()
```

Each `Processor` class must implement the `process` method and might optionally implement the
`on_start` and `on_exit` methods to setup and tear down any resources required during the pipeline
activity (these will be called just once).

The above example sets up a `Producer` that generates 1000 numbers, a `Multiplier` class that simply
multiplies by two a number and finally a `Consumer` class that displays the total number of items
processed.

Note the **use** of the `yield` keyword for the `Producer.process` method. All producers (that is:
the class that appears at the beginning of a `Pipeline`) need to produce values through `yield`.
While the example shows only one `Producer` created in the pipeline, it is possible and valid to
use as many producers as you want.

What happens if an exception is raised? Any errors produced by either a consumer/producer or consumer
stage it will be put in an error queue. By default `Pipeline` outputs all errors to `stderr`. If you
need to customize error handling you need to inherit from the `ErrorWatcher` class and at least implement
its `process` method. For example:

```python
class CustomErrorWatcher(ErrorWatcher):
    def process(self, error):
        ...
```

where `error` is an exception that was raised somewhere in the pipeline. In order to complete this
custom error handling you need to supply this class to the `Pipeline` constructor through the
`error_watcher` keyword argument:

```python
Pipeline([
    ...
], error_watcher=CustomErrorWatcher).run()
```


Licence
-------

SimplePipeline is distributed under the [GNU LGPLv3](https://www.gnu.org/licenses/lgpl.txt) license.


Authors
-------

* Lucas Liendo.
