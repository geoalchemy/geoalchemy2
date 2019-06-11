from setuptools import setup, find_packages


version = '0.6.3'

setup(
    name='GeoAlchemy2',
    version=version,
    description="Using SQLAlchemy with Spatial Databases",
    long_description=open('README.rst').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Plugins",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords='geo gis sqlalchemy orm',
    author='Eric Lemoine',
    author_email='eric.lemoine@gmail.com',
    url='http://geoalchemy.org/',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests', "doc"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLAlchemy>=0.8',
    ],
    entry_points="""
    # -*- Entry points: -*-
    """,
)
