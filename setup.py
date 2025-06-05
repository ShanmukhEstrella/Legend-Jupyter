from setuptools import setup

setup(
    name='pure_kernel',
    version='0.1',
    packages=['pure'],
    install_requires=['ipykernel'],
    entry_points={
        'console_scripts': ['pure_kernel_install = pure.__init__:main'],
    }
)