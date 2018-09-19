from setuptools import setup

setup(name='amqp_python',
      version='0.1',
      description='Simple amqp endpoint for python, based on pika',
      url='http://github.com/2trde/amqp_dsl',
      author='Daniel Kirch',
      author_email='daniel.kirch@2trde.com',
      packages=['amqp_python'],
      install_requires=[
        'pika'
      ])
