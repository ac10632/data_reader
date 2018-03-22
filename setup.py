from distutils.core import setup

setup(
    name='data_reader',
    version='1.2',
    packages=['data_reader','data_reader.reader'],
    package_data={'data_reader': ['data/*.dat','test_data/*'],'data_reader.reader': ['data/*'] },
    url='',
    license='MIT',
    author='William Alexander',
    author_email='worksprogress1@gmail.com',
    description='tools for handling modeling data',
    requires=['numpy','tensorflow']
)
