from setuptools import setup

setup(name='amqpy',
      version='0.2',
      description='Simple amqp endpoint for python, based on pika',
      url='http://github.com/2trde/amqp_dsl',
      author='Daniel Kirch',
      author_email='daniel.kirch@2trde.com',
      packages=['amqpy'],
      install_requires=[
        'pika'
      ])
