from setuptools import setup

install_requires = []

with open('./requirements.txt') as requirements_txt:
    requirements = requirements_txt.read().strip().splitlines()
    for requirement in requirements:
        if requirement.startswith('#'):
            continue
        elif requirement.startswith('-e '):
            install_requires.append(requirement.split('=')[1])
        else:
            install_requires.append(requirement)

setup(
    name='ocdsdata',
    version='0.0.0',
    author='David Raznick',
    author_email='david.raznick@opendataservices.coop',
    scripts=['ocdsdata.py'],
    url='https://github.com/DataServices/ocdsdata',
    description='',
    packages=[
        'kingfisher-collect',
        'kingfisher-collect.kingfisher_scrapy',
        'kingfisher-collect.kingfisher_scrapy.spiders',
    ],
    package_data={
        'kingfisher-collect.kingfisher_scrapy': ['item_schema/*.json'],
        'kingfisher-collect': ['scrapy.cfg'],
    },
    include_package_data=True,
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
    ],
    py_modules = ['ocdsdata'],
    install_requires=install_requires,
)
