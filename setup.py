from setuptools import setup

install_requires = []

setup(
    name='ocdsdata',
    version='0.0.0',
    author='David Raznick',
    author_email='david.raznick@opendataservices.coop',
    scripts=['ocdsdata.py'],
    url='https://github.com/DataServices/ocdsdata',
    description='',
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
    ],
    py_modules=['ocdsdata'],
    install_requires=install_requires,
)
