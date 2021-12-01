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

Copyleft 2021 - present Lucas Liendo.
"""

from setuptools import setup, find_packages


setup(
    name='SimplePipeline',
    description='A ridiculously simple multi-processing pipeline module',
    version='0.0.1',
    packages=find_packages(),
    author='Lucas Liendo',
    author_email='liendolucas84@gmail.com',
    keywords='pipeline multi-processing',
    license='LGPLv3',
    zip_safe=False,
    url='https://github.com/lliendo/SimplePipeline',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python 3.6',
    ],
)
