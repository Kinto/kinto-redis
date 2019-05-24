import codecs
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    README = f.read()

with codecs.open(os.path.join(here, 'CHANGELOG.rst'), encoding='utf-8') as f:
    CHANGELOG = f.read()

REQUIREMENTS = [
    'kinto>=13.0.0',
    'redis',
]

setup(name='kinto-redis',
      version='2.0.1',
      description='Kinto Redis',
      long_description=README + '\r\n' + CHANGELOG,
      license='Apache License (2.0)',
      classifiers=[
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
          "License :: OSI Approved :: Apache Software License",
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
      ],
      keywords="web services",
      author='Mozilla Services',
      author_email='services-dev@mozilla.com',
      url='https://github.com/Kinto/kinto-redis',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=REQUIREMENTS)
