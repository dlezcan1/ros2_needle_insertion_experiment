from setuptools import setup

package_name = 'experiment_ros2bag'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='dlezcan1',
    maintainer_email='dlezcan1@jhu.edu',
    description='Package for post experiment processing',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            "process_bag = experiment_ros2bag.process_bag:main",
        ],
    },
)
