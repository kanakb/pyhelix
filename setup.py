import setuptools

import pyhelix.constants as constants


def readme():
    with open('README.md') as f:
        return f.read()

setuptools.setup(
    name='pyhelix',
    version=str(constants.CURRENT_VERSION),
    description='Python bindings for Apache Helix',
    long_description=('Support for creating Helix participants in Python.'
                      ' Register a state model, connect, and go!'),
    url='http://github.com/kanakb/pyhelix',
    license='Apache License 2.0',
    packages=setuptools.find_packages(),
    keywords='helix',
    install_requires=['kazoo', 'futures', 'argparse'],
    include_package_data=True,
    test_suite='nose.collector',
    tests_require=['nose'],
    author='Kanak Biscuitwala, Zhen Zhang',
    author_email='kanak.b@hotmail.com, nehzgnahz@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.6',
        ],
    zip_safe=False)
