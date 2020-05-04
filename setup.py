from setuptools import setup, find_packages
from distutils.util import convert_path
import package

main_ns = {}
ver_path = convert_path('package.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

setup(
    name="siirto",
    version=package.__version__,
    author="lokeshlal@gmail.com",
    description="database migration utility",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    entry_points={
        'console_scripts': [
            'siirto = siirto.cli.cli:main'
        ]
    },
    python_requires='>=3.6',
    include_package_data=True,
    platforms="any",
    zip_safe=False,
    install_requires=package.install_requires
)
